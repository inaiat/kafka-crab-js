use napi::{bindgen_prelude::*, threadsafe_function::ThreadsafeFunction};

use rdkafka::{
  consumer::{stream_consumer::StreamConsumer, CommitMode as RdKfafkaCommitMode, Consumer},
  message::{BorrowedMessage, Header, OwnedHeaders},
  producer::FutureRecord,
  Message,
};

use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::kafka::{
  consumer::kafka_consumer::RETRY_COUNTER_NAME,
  kafka_util::{
    kakfa_headers_to_hashmap, kakfa_headers_to_hashmap_buffer, ExtractValueOnKafkaHashMap,
  },
  model::Payload,
};

use super::kafka_consumer::{ConsumerResult, ProducerHelper};

use super::consumer_context::CustomContext;

pub struct ConsumerThread {
  pub stream_consumer: StreamConsumer<CustomContext>,
  pub func: ThreadsafeFunction<Payload>,
  pub commit_mode: Option<RdKfafkaCommitMode>,
  pub retries: i32,
  pub next_topic_on_fail: Option<String>,
  pub producer: ProducerHelper,
  pub pause_consumer_duration: Option<Duration>,
}

impl ConsumerThread {
  pub async fn start(&self) {
    loop {
      if let Some(duration) = &self.pause_consumer_duration {
        tracing::debug!("Pause consumer for {}", duration.as_millis());
        tokio::time::sleep(*duration).await;
      };
      match self.stream_consumer.recv().await {
        Err(e) => error!("Error while receiving from stream consumer: {:?}", e),
        Ok(message) => {
          let message_result = self.process_message(&message).await;
          match self.handle_message_result(&message, message_result).await {
            Ok(_) => {
              debug!(
                "Message consumed successfully. Topic: {}, Partition: {}, Offset: {}",
                message.topic(),
                message.partition(),
                message.offset()
              );
            }
            Err(err) => {
              error!("Message processing failed. Error: {:?}", err);
            }
          }
        }
      };
    }
  }

  async fn process_message(
    &self,
    message: &BorrowedMessage<'_>,
  ) -> anyhow::Result<Option<ConsumerResult>> {
    match message.payload_view::<[u8]>() {
      None => Ok(None),
      Some(Ok(payload)) => {
        let payload_js = create_payload(message, payload);
        match self
          .func
          .call_async::<Promise<Option<ConsumerResult>>>(Ok(payload_js))
          .await
        {
          Ok(js_result) => match js_result.await {
            Ok(value) => match value {
              None | Some(ConsumerResult::Ok) => {
                if let Some(commit_mode) = self.commit_mode {
                  match self.stream_consumer.commit_message(message, commit_mode) {
                    Ok(_) => Ok(Some(ConsumerResult::Ok)),
                    Err(e) => Err(anyhow::Error::new(e)),
                  }
                } else {
                  Ok(Some(ConsumerResult::Ok))
                }
              }
              Some(ConsumerResult::Retry) => Ok(Some(ConsumerResult::Retry)),
            },
            Err(_) => {
              warn!("Error while calling return function");
              Ok(Some(ConsumerResult::Ok))
            }
          },
          Err(err) => {
            warn!("Error while calling function: {:?}", err.to_string());
            Ok(Some(ConsumerResult::Retry))
          }
        }
      }
      Some(_) => Err(anyhow::Error::msg(
        "Error while deserializing message payload",
      )),
    }
  }

  async fn handle_message_result(
    &self,
    message: &BorrowedMessage<'_>,
    message_result: anyhow::Result<Option<ConsumerResult>>,
  ) -> anyhow::Result<()> {
    match message_result {
      Ok(consumer_result) => match consumer_result {
        Some(result) => match result {
          ConsumerResult::Ok => Ok(()),
          ConsumerResult::Retry => match self.send_to_retry_strategy(message).await {
            Ok(_) => {
              debug!("Message sent to retry strategy");
              Ok(())
            }
            Err(e) => Err(anyhow::Error::msg(format!(
              "Error while sending message to retry strategy: {:?}",
              e
            ))),
          },
        },
        None => Ok(()),
      },
      Err(err) => Err(err),
    }
  }

  async fn send_to_retry_strategy(&self, message: &BorrowedMessage<'_>) -> anyhow::Result<()> {
    let retry_counter: usize = {
      let headers_map = kakfa_headers_to_hashmap(message.headers());
      let mut counter = headers_map.get_value(RETRY_COUNTER_NAME).unwrap_or(1);
      if self.retries > 0 {
        counter += 1;
      }
      counter
    };

    let next_topic = if self.retries > 0 && retry_counter <= self.retries.try_into().unwrap() {
      Some(message.topic().to_string())
    } else {
      self.next_topic_on_fail.clone()
    };

    match next_topic {
      Some(next_topic) => {
        info!("Message sent to next topic: {}", next_topic);
        let new_headers = message
          .headers()
          .unwrap_or(OwnedHeaders::new().as_borrowed())
          .detach()
          .insert(Header {
            key: RETRY_COUNTER_NAME,
            value: Some(retry_counter.to_string().as_str()),
          });
        let key = message.key().unwrap_or(&[]);

        let record = FutureRecord::to(&next_topic)
          .key(key)
          .headers(new_headers)
          .payload(message.payload().unwrap_or(&[]));

        let _result = self
          .producer
          .future_producer
          .send(record, self.producer.queue_timeout)
          .await
          .map_err(|e| anyhow::Error::new(e.0))?;
        Ok(())
      }
      None => {
        debug!("There is no next topic. Commit message");
        Ok(())
      }
    }
  }
}

fn create_payload(message: &BorrowedMessage<'_>, payload: &[u8]) -> Payload {
  let key: Option<Buffer> = message.key().map(|bytes| bytes.into());
  let headers = Some(kakfa_headers_to_hashmap_buffer(message.headers()));
  let payload_js = Payload::new(
    payload.into(),
    key,
    headers,
    message.topic().to_owned(),
    message.partition(),
    message.offset(),
  );
  payload_js
}

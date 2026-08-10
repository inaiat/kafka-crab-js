#![cfg_attr(test, allow(dead_code))]

use napi::{
  bindgen_prelude::{AsyncTask, Buffer},
  Env, Result, Task,
};

use super::{
  document::{create_pdf_bytes, IncrementalPdf, PdfDocumentState},
  image::image_dimensions,
  input::{
    CreatePdfInput, PdfAnnotationInput, PdfDocumentBuilderInput, PdfElementInput,
    PdfFontRegistrationInput, PdfPageInput, PdfPageSetupInput, PdfTextLineInput,
  },
  validation::invalid_arg,
};

#[napi(object)]
pub struct PdfImageInfo {
  pub width: u32,
  pub height: u32,
}

#[napi]
pub fn get_image_dimensions(data: Buffer) -> Result<PdfImageInfo> {
  let (width, height) = image_dimensions(&data)?;
  Ok(PdfImageInfo { width, height })
}

#[napi]
pub fn create_pdf(input: CreatePdfInput) -> Result<Buffer> {
  create_pdf_bytes(input).map(Buffer::from)
}

#[napi]
pub fn create_pdf_stream(
  input: CreatePdfInput,
  start_after_header: Option<bool>,
) -> Result<PdfOutput> {
  let pages = input
    .pages
    .ok_or_else(|| invalid_arg("pages must contain at least one page"))?;
  if pages.is_empty() {
    return Err(invalid_arg("pages must contain at least one page"));
  }
  let mut state = PdfDocumentState::new(input.title, input.unit, input.metadata)?;
  for (index, page) in pages.into_iter().enumerate() {
    state.add_page(page, &format!("pages[{index}]"))?;
  }
  Ok(PdfOutput {
    state: Some(state.finish_stream(start_after_header.unwrap_or(false))?),
  })
}

#[napi(ts_return_type = "Promise<Buffer>")]
pub fn create_pdf_async(input: CreatePdfInput) -> AsyncTask<CreatePdfTask> {
  AsyncTask::new(CreatePdfTask { input: Some(input) })
}

#[napi]
pub struct PdfDocumentBuilder {
  state: Option<PdfDocumentState>,
}

#[napi]
impl PdfDocumentBuilder {
  #[napi(constructor)]
  pub fn new(input: Option<PdfDocumentBuilderInput>) -> Result<Self> {
    let (title, unit, metadata) = match input {
      Some(input) => (input.title, input.unit, input.metadata),
      None => (None, None, None),
    };

    Ok(Self {
      state: Some(PdfDocumentState::new(title, unit, metadata)?),
    })
  }

  #[napi]
  pub fn start_page(&mut self, page: PdfPageSetupInput) -> Result<()> {
    self
      .state_mut()?
      .start_page(page.width, page.height, "currentPage")
  }

  #[napi]
  pub fn register_font(&mut self, input: PdfFontRegistrationInput) -> Result<()> {
    self.state_mut()?.register_font(input)
  }

  #[napi]
  pub fn layout_text(
    &mut self,
    text: String,
    font: String,
    font_size: f64,
    width: f64,
    hyphenate: Option<bool>,
  ) -> Result<Vec<PdfTextLineInput>> {
    self
      .state_mut()?
      .layout_text(&text, &font, font_size, width, hyphenate.unwrap_or(false))
  }

  #[napi]
  pub fn measure_texts(
    &mut self,
    texts: Vec<String>,
    font: String,
    font_size: f64,
  ) -> Result<Vec<f64>> {
    self.state_mut()?.measure_texts(texts, &font, font_size)
  }

  #[napi]
  pub fn append_elements(&mut self, elements: Vec<PdfElementInput>) -> Result<()> {
    self.state_mut()?.append_elements(elements)
  }

  #[napi]
  pub fn append_annotations(&mut self, annotations: Vec<PdfAnnotationInput>) -> Result<()> {
    self.state_mut()?.append_annotations(annotations)
  }

  #[napi]
  pub fn end_page(&mut self) -> Result<()> {
    self.state_mut()?.end_page()
  }

  #[napi]
  pub fn add_page(&mut self, page: PdfPageInput) -> Result<()> {
    self.state_mut()?.add_page(page, "page")
  }

  #[napi]
  pub fn add_pages(&mut self, pages: Vec<PdfPageInput>) -> Result<()> {
    let state = self.state_mut()?;

    for (index, page) in pages.into_iter().enumerate() {
      state.add_page(page, &format!("pages[{index}]"))?;
    }

    Ok(())
  }

  #[napi]
  pub fn finish(&mut self) -> Result<Buffer> {
    self.take_state()?.finish().map(Buffer::from)
  }

  #[napi(ts_return_type = "Promise<Buffer>")]
  pub fn finish_async(&mut self) -> Result<AsyncTask<FinishPdfDocumentTask>> {
    Ok(AsyncTask::new(FinishPdfDocumentTask {
      state: Some(self.take_state()?),
    }))
  }

  #[napi]
  pub fn finish_stream(&mut self, start_after_header: Option<bool>) -> Result<PdfOutput> {
    Ok(PdfOutput {
      state: Some(
        self
          .take_state()?
          .finish_stream(start_after_header.unwrap_or(false))?,
      ),
    })
  }

  fn state_mut(&mut self) -> Result<&mut PdfDocumentState> {
    self
      .state
      .as_mut()
      .ok_or_else(|| invalid_arg("PdfDocumentBuilder has already finished"))
  }

  fn take_state(&mut self) -> Result<PdfDocumentState> {
    self
      .state
      .take()
      .ok_or_else(|| invalid_arg("PdfDocumentBuilder has already finished"))
  }
}

#[napi]
pub struct PdfOutput {
  state: Option<IncrementalPdf>,
}

#[napi]
impl PdfOutput {
  // Keep streaming pull-driven instead of storing a JS callback. NAPI callbacks
  // need FunctionRef/ThreadsafeFunction lifetime management once they outlive
  // the call; a synchronous next_chunk boundary gives us the same backpressure
  // semantics in Node and zero-config single-threaded WASM.
  #[napi]
  pub fn next_chunk(&mut self, chunk_size: Option<u32>) -> Result<Option<Buffer>> {
    let state = self
      .state
      .as_mut()
      .ok_or_else(|| invalid_arg("PdfOutput has been cancelled or consumed"))?;
    let bytes = state.next_chunk(chunk_size.unwrap_or(64 * 1024) as usize)?;
    Ok(bytes.map(Buffer::from))
  }

  #[napi]
  pub fn cancel(&mut self) {
    if let Some(state) = self.state.as_mut() {
      state.cancel();
    }
    self.state = None;
  }
}

pub(super) struct CreatePdfTask {
  input: Option<CreatePdfInput>,
}

impl Task for CreatePdfTask {
  type Output = Vec<u8>;
  type JsValue = Buffer;

  fn compute(&mut self) -> Result<Self::Output> {
    let input = self
      .input
      .take()
      .ok_or_else(|| invalid_arg("createPdfAsync input was already consumed"))?;
    create_pdf_bytes(input)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(Buffer::from(output))
  }
}

pub struct FinishPdfDocumentTask {
  state: Option<PdfDocumentState>,
}

impl Task for FinishPdfDocumentTask {
  type Output = Vec<u8>;
  type JsValue = Buffer;

  fn compute(&mut self) -> Result<Self::Output> {
    let state = self
      .state
      .take()
      .ok_or_else(|| invalid_arg("finishAsync input was already consumed"))?;
    state.finish()
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(Buffer::from(output))
  }
}

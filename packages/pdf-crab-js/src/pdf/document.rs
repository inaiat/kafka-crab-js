#![cfg_attr(test, allow(dead_code))]

use miniz_oxide::deflate::compress_to_vec_zlib;
use napi::Result;
use pdf_writer::types::{ActionType, AnnotationType, BorderType, TrappingStatus};
use pdf_writer::{Chunk, Content, Filter, Finish, Rect, Ref, Str, TextStr};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::{
  color::optional_color,
  elements::{append_element, AppendElementContext, PreparedImageData},
  font::{BuiltinFont, FontRegistry, FontResource, PreparedFontObject},
  image::write_image_xobject,
  input::{
    CreatePdfInput, PdfAnnotationInput, PdfElementInput, PdfMetadataInput, PdfPageInput,
    PdfTextLineInput,
  },
  unit::Unit,
  validation::{invalid_arg, positive_f32, required, required_f32, required_positive_f32},
};

const DEFAULT_TITLE: &str = "pdf-crab-js";
const CATALOG_REF: i32 = 1;
const PAGES_REF: i32 = 2;
const INFO_REF: i32 = 3;
const FIRST_PAGE_REF: i32 = 18;
const PDF_HEADER: &[u8] = b"%PDF-1.7\n%\x80\x80\x80\x80\n\n";

pub(super) fn create_pdf_bytes(input: CreatePdfInput) -> Result<Vec<u8>> {
  let pages = input.pages.ok_or_else(|| required("pages"))?;

  if pages.is_empty() {
    return Err(invalid_arg("pages must contain at least one page"));
  }

  let mut document = PdfDocumentState::new(input.title, input.unit, input.metadata)?;

  for (page_index, page) in pages.into_iter().enumerate() {
    document.add_page(page, &format!("pages[{page_index}]"))?;
  }

  document.finish()
}

pub(super) struct PdfDocumentState {
  title: String,
  metadata: Option<PdfMetadataInput>,
  unit: Unit,
  prepared_pages: Vec<PreparedPage>,
  fonts: FontRegistry,
  next_ref: i32,
  open_page: Option<OpenPage>,
}

impl PdfDocumentState {
  pub(super) fn new(
    title: Option<String>,
    unit: Option<String>,
    metadata: Option<PdfMetadataInput>,
  ) -> Result<Self> {
    Ok(Self {
      title: title.unwrap_or_else(|| DEFAULT_TITLE.to_string()),
      metadata,
      unit: Unit::from_input(unit)?,
      prepared_pages: Vec::new(),
      fonts: FontRegistry::default(),
      next_ref: FIRST_PAGE_REF,
      open_page: None,
    })
  }

  pub(super) fn start_page(&mut self, width: f64, height: f64, path: &str) -> Result<()> {
    if self.open_page.is_some() {
      return Err(invalid_arg(
        "cannot start a new page while another page is open; call endPage() first",
      ));
    }

    let width = self
      .unit
      .coordinate(positive_f32(width, &format!("{path}.width"))?);
    let height = self
      .unit
      .coordinate(positive_f32(height, &format!("{path}.height"))?);
    let reference = take_ref(&mut self.next_ref);
    let content_ref = take_ref(&mut self.next_ref);

    self.open_page = Some(OpenPage {
      path: path.to_string(),
      reference,
      content_ref,
      width,
      height,
      content: Content::new(),
      annotations: Vec::new(),
      images: Vec::new(),
      fonts: BTreeSet::new(),
      element_count: 0,
      annotation_count: 0,
    });

    Ok(())
  }

  pub(super) fn append_elements(&mut self, elements: Vec<PdfElementInput>) -> Result<()> {
    let page = self
      .open_page
      .as_mut()
      .ok_or_else(|| invalid_arg("appendElements requires an open page; call startPage() first"))?;

    for element in elements {
      let mut context = AppendElementContext {
        unit: self.unit,
        page_height: page.height,
        images: &mut page.images,
        fonts: &mut self.fonts,
        used_fonts: &mut page.fonts,
        next_ref: &mut self.next_ref,
      };
      append_element(
        &mut page.content,
        element,
        &mut context,
        &format!("{}.elements[{}]", page.path, page.element_count),
      )?;
      page.element_count += 1;
    }

    Ok(())
  }

  pub(super) fn append_annotations(&mut self, annotations: Vec<PdfAnnotationInput>) -> Result<()> {
    let (page_ref, page_height, page_path, start_index) = {
      let page = self.open_page.as_ref().ok_or_else(|| {
        invalid_arg("appendAnnotations requires an open page; call startPage() first")
      })?;
      (
        page.reference,
        page.height,
        page.path.clone(),
        page.annotation_count,
      )
    };
    let mut prepared_annotations = Vec::with_capacity(annotations.len());

    for (index, annotation) in annotations.into_iter().enumerate() {
      let annotation_ref = take_ref(&mut self.next_ref);
      let annotation = prepare_annotation(
        annotation,
        self.unit,
        page_height,
        page_ref,
        &format!("{page_path}.annotations[{}]", start_index + index),
      )?;

      prepared_annotations.push(PreparedAnnotation {
        reference: annotation_ref,
        annotation,
      });
    }

    let page = self.open_page.as_mut().ok_or_else(|| {
      invalid_arg("appendAnnotations requires an open page; call startPage() first")
    })?;
    page.annotation_count += prepared_annotations.len();
    page.annotations.extend(prepared_annotations);

    Ok(())
  }

  pub(super) fn end_page(&mut self) -> Result<()> {
    let page = self
      .open_page
      .take()
      .ok_or_else(|| invalid_arg("endPage requires an open page; call startPage() first"))?;

    self.prepared_pages.push(PreparedPage {
      reference: page.reference,
      content_ref: page.content_ref,
      width: page.width,
      height: page.height,
      content: Some(page.content.finish().into_vec()),
      annotations: page.annotations,
      images: page.images,
      fonts: page.fonts,
    });

    Ok(())
  }

  pub(super) fn add_page(&mut self, page: PdfPageInput, path: &str) -> Result<()> {
    self.start_page(page.width, page.height, path)?;

    if let Some(elements) = page.elements {
      self.append_elements(elements)?;
    }
    if let Some(annotations) = page.annotations {
      self.append_annotations(annotations)?;
    }

    self.end_page()
  }

  pub(super) fn finish(self) -> Result<Vec<u8>> {
    let mut output = self.finish_stream(false)?;
    let mut bytes = Vec::new();
    while let Some(chunk) = output.next_chunk(64 * 1024)? {
      bytes.extend(chunk);
    }
    Ok(bytes)
  }

  pub(super) fn finish_stream(self, start_after_header: bool) -> Result<IncrementalPdf> {
    if self.open_page.is_some() {
      return Err(invalid_arg(
        "cannot finish while a page is open; call endPage() first",
      ));
    }

    if self.prepared_pages.is_empty() {
      return Err(invalid_arg("pages must contain at least one page"));
    }

    let max_ref = self.next_ref - 1;
    Ok(IncrementalPdf::new(
      self.title,
      self.metadata,
      self.prepared_pages,
      self.fonts,
      max_ref,
      start_after_header,
    ))
  }

  pub(super) fn register_font(
    &mut self,
    input: super::input::PdfFontRegistrationInput,
  ) -> Result<()> {
    self.fonts.register(input, &mut self.next_ref)
  }

  pub(super) fn layout_text(
    &mut self,
    text: &str,
    font: &str,
    font_size: f64,
    width: f64,
    hyphenate: bool,
  ) -> Result<Vec<PdfTextLineInput>> {
    let font_size = positive_f32(font_size, "fontSize")?;
    let width = self.unit.coordinate(positive_f32(width, "width")?);
    self
      .fonts
      .wrap_text(font, text, width, font_size, hyphenate, "text")
  }

  pub(super) fn measure_texts(
    &mut self,
    texts: Vec<String>,
    font: &str,
    font_size: f64,
  ) -> Result<Vec<f64>> {
    let font_size = positive_f32(font_size, "fontSize")?;
    texts
      .iter()
      .map(|text| {
        self
          .fonts
          .measure_text(font, text, font_size, "text")
          .map(f64::from)
      })
      .collect()
  }
}

#[derive(Clone, Copy)]
enum StreamPhase {
  Header,
  Catalog,
  Pages,
  Info,
  Font(usize),
  EmbeddedFont(usize),
  Page(usize),
  Content(usize),
  Annotation(usize, usize),
  Image(usize, usize, bool),
  Xref,
  Done,
}

/// Pull-driven PDF serializer. Layout owns page content, while this serializer
/// materializes at most one PDF object and one requested output chunk at a time.
pub(super) struct IncrementalPdf {
  title: String,
  metadata: Option<PdfMetadataInput>,
  pages: Vec<PreparedPage>,
  fonts: FontRegistry,
  builtin_fonts: Vec<BuiltinFont>,
  embedded_font_objects: VecDeque<PreparedFontObject>,
  phase: StreamPhase,
  pending: Vec<u8>,
  pending_offset: usize,
  written: usize,
  offsets: BTreeMap<i32, usize>,
  max_ref: i32,
}

impl IncrementalPdf {
  fn new(
    title: String,
    metadata: Option<PdfMetadataInput>,
    pages: Vec<PreparedPage>,
    fonts: FontRegistry,
    max_ref: i32,
    start_after_header: bool,
  ) -> Self {
    let builtin_fonts = fonts.used_builtins().collect();
    Self {
      title,
      metadata,
      pages,
      fonts,
      builtin_fonts,
      embedded_font_objects: VecDeque::new(),
      phase: if start_after_header {
        StreamPhase::Catalog
      } else {
        StreamPhase::Header
      },
      pending: Vec::new(),
      pending_offset: 0,
      written: if start_after_header {
        PDF_HEADER.len()
      } else {
        0
      },
      offsets: BTreeMap::new(),
      max_ref,
    }
  }

  pub(super) fn cancel(&mut self) {
    self.phase = StreamPhase::Done;
    self.pending.clear();
    self.pages.clear();
    self.embedded_font_objects.clear();
    self.offsets.clear();
  }

  pub(super) fn next_chunk(&mut self, requested_size: usize) -> Result<Option<Vec<u8>>> {
    let chunk_size = requested_size.max(1);

    loop {
      if self.pending_offset < self.pending.len() {
        let end = (self.pending_offset + chunk_size).min(self.pending.len());
        let chunk = self.pending[self.pending_offset..end].to_vec();
        self.pending_offset = end;
        self.written += chunk.len();
        if self.pending_offset == self.pending.len() {
          self.pending = Vec::new();
          self.pending_offset = 0;
        }
        return Ok(Some(chunk));
      }

      if matches!(self.phase, StreamPhase::Done) {
        return Ok(None);
      }

      self.produce_next()?;
    }
  }

  fn set_pending(&mut self, id: Option<Ref>, bytes: Vec<u8>) {
    if let Some(id) = id {
      self.offsets.insert(id.get(), self.written);
    }
    self.pending = bytes;
    self.pending_offset = 0;
  }

  fn object<F>(&mut self, id: Ref, writer: F)
  where
    F: FnOnce(&mut Chunk),
  {
    let mut chunk = Chunk::new();
    writer(&mut chunk);
    self.set_pending(Some(id), chunk.as_bytes().to_vec());
  }

  fn produce_next(&mut self) -> Result<()> {
    let phase = std::mem::replace(&mut self.phase, StreamPhase::Done);
    match phase {
      StreamPhase::Header => {
        self.set_pending(None, PDF_HEADER.to_vec());
        self.phase = StreamPhase::Catalog;
      }
      StreamPhase::Catalog => {
        self.object(Ref::new(CATALOG_REF), |chunk| {
          chunk
            .indirect(Ref::new(CATALOG_REF))
            .start::<pdf_writer::writers::Catalog>()
            .pages(Ref::new(PAGES_REF));
        });
        self.phase = StreamPhase::Pages;
      }
      StreamPhase::Pages => {
        let page_refs: Vec<_> = self.pages.iter().map(|page| page.reference).collect();
        let count = self.pages.len() as i32;
        self.object(Ref::new(PAGES_REF), |chunk| {
          chunk
            .pages(Ref::new(PAGES_REF))
            .kids(page_refs)
            .count(count);
        });
        self.phase = StreamPhase::Info;
      }
      StreamPhase::Info => {
        let title = self.title.clone();
        let metadata = self.metadata.clone();
        self.object(Ref::new(INFO_REF), |chunk| {
          write_document_info(chunk, Ref::new(INFO_REF), &title, metadata);
        });
        self.phase = StreamPhase::Font(0);
      }
      StreamPhase::Font(index) => {
        if let Some(font) = self.builtin_fonts.get(index) {
          let id = Ref::new(font.ref_number());
          let base_name = font.base_name();
          self.object(id, |chunk| {
            chunk
              .type1_font(id)
              .base_font(base_name)
              .encoding_predefined(pdf_writer::Name(b"WinAnsiEncoding"));
          });
          self.phase = StreamPhase::Font(index + 1);
        } else {
          self.phase = StreamPhase::EmbeddedFont(0);
          return self.produce_next();
        }
      }
      StreamPhase::EmbeddedFont(index) => {
        if let Some(object) = self.embedded_font_objects.pop_front() {
          self.set_pending(Some(object.reference), object.bytes);
          self.phase = StreamPhase::EmbeddedFont(index);
        } else if index < self.fonts.custom_len() {
          if self.fonts.custom_is_used(index) {
            self.embedded_font_objects = self.fonts.prepare_custom_objects(index)?.into();
          }
          self.phase = StreamPhase::EmbeddedFont(index + 1);
          return self.produce_next();
        } else {
          self.phase = StreamPhase::Page(0);
          return self.produce_next();
        }
      }
      StreamPhase::Page(index) => {
        if let Some(page) = self.pages.get(index) {
          let page_ref = page.reference;
          let content_ref = page.content_ref;
          let width = page.width;
          let height = page.height;
          let annotations = page
            .annotations
            .iter()
            .map(|annotation| annotation.reference);
          let images = page
            .images
            .iter()
            .map(|image| (image.name.clone(), image.image_ref));
          let bytes = {
            let mut chunk = Chunk::new();
            let mut page_writer = chunk.page(page_ref);
            page_writer.parent(Ref::new(PAGES_REF));
            page_writer.media_box(Rect::new(0.0, 0.0, width, height));
            page_writer.contents(content_ref);
            if !page.annotations.is_empty() {
              page_writer.annotations(annotations);
            }
            let mut resources = page_writer.resources();
            if !page.fonts.is_empty() {
              let mut fonts = resources.fonts();
              for font in &page.fonts {
                fonts.pair(
                  pdf_writer::Name(font.name.as_slice()),
                  Ref::new(font.ref_number),
                );
              }
              fonts.finish();
            }
            if !page.images.is_empty() {
              let mut x_objects = resources.x_objects();
              for (name, image_ref) in images {
                x_objects.pair(pdf_writer::Name(name.as_slice()), image_ref);
              }
              x_objects.finish();
            }
            resources.finish();
            page_writer.finish();
            chunk.as_bytes().to_vec()
          };
          self.set_pending(Some(page_ref), bytes);
          self.phase = StreamPhase::Content(index);
        } else {
          self.phase = StreamPhase::Xref;
          return self.produce_next();
        }
      }
      StreamPhase::Content(index) => {
        let page = &mut self.pages[index];
        let id = page.content_ref;
        let content = page.content.take().unwrap_or_default();
        let compressed = compress_to_vec_zlib(&content, 6);
        self.object(id, |chunk| {
          chunk.stream(id, &compressed).filter(Filter::FlateDecode);
        });
        self.phase = if self.pages[index].annotations.is_empty() {
          if self.pages[index].images.is_empty() {
            StreamPhase::Page(index + 1)
          } else {
            StreamPhase::Image(index, 0, false)
          }
        } else {
          StreamPhase::Annotation(index, 0)
        };
      }
      StreamPhase::Annotation(page_index, annotation_index) => {
        let page = &self.pages[page_index];
        if let Some(annotation) = page.annotations.get(annotation_index) {
          let id = annotation.reference;
          let next_phase = if annotation_index + 1 < page.annotations.len() {
            StreamPhase::Annotation(page_index, annotation_index + 1)
          } else if page.images.is_empty() {
            StreamPhase::Page(page_index + 1)
          } else {
            StreamPhase::Image(page_index, 0, false)
          };
          let value = LinkAnnotation {
            page_ref: annotation.annotation.page_ref,
            rect: annotation.annotation.rect,
            url: annotation.annotation.url.clone(),
            color: annotation.annotation.color,
          };
          self.object(id, |chunk| write_annotation(chunk, id, &value));
          self.phase = next_phase;
        }
      }
      StreamPhase::Image(page_index, image_index, mask) => {
        let (id, next_phase, bytes) = {
          let page = &self.pages[page_index];
          if let Some(image) = page.images.get(image_index) {
            let id = if mask {
              image.mask_ref
            } else {
              Some(image.image_ref)
            };
            let next_phase = if mask || image.mask_ref.is_none() {
              if image_index + 1 < page.images.len() {
                StreamPhase::Image(page_index, image_index + 1, false)
              } else {
                StreamPhase::Page(page_index + 1)
              }
            } else {
              StreamPhase::Image(page_index, image_index, true)
            };
            let bytes = id.map(|id| {
              let decoded = &image.decoded;
              let mut chunk = Chunk::new();
              if mask {
                if let Some(alpha) = decoded.alpha.as_ref() {
                  let mut mask_writer = chunk.image_xobject(id, alpha);
                  mask_writer.filter(pdf_writer::Filter::FlateDecode);
                  mask_writer.width(decoded.width as i32);
                  mask_writer.height(decoded.height as i32);
                  mask_writer.color_space().device_gray();
                  mask_writer.bits_per_component(8);
                  mask_writer.finish();
                }
              } else {
                write_image_xobject(&mut chunk, image.image_ref, image.mask_ref, decoded);
              }
              chunk.as_bytes().to_vec()
            });
            (id, next_phase, bytes)
          } else {
            (None, StreamPhase::Page(page_index + 1), None)
          }
        };
        if let (Some(id), Some(bytes)) = (id, bytes) {
          self.set_pending(Some(id), bytes);
        }
        self.phase = next_phase;
      }
      StreamPhase::Xref => {
        let xref_offset = self.written;
        let size = self.max_ref + 1;
        let mut bytes = format!("xref\n0 {size}\n0000000000 65535 f\r\n").into_bytes();
        for id in 1..size {
          if let Some(offset) = self.offsets.get(&id) {
            bytes.extend(format!("{offset:010} 00000 n\r\n").as_bytes());
          } else {
            bytes.extend(b"0000000000 00000 f\r\n");
          }
        }
        bytes.extend(
          format!(
            "trailer\n<<\n/Size {size}\n/Root {CATALOG_REF} 0 R\n/Info {INFO_REF} 0 R\n>>\nstartxref\n{xref_offset}\n%%EOF\n"
          )
          .as_bytes(),
        );
        self.set_pending(None, bytes);
        self.phase = StreamPhase::Done;
      }
      StreamPhase::Done => {}
    }
    Ok(())
  }
}

struct OpenPage {
  path: String,
  reference: Ref,
  content_ref: Ref,
  width: f32,
  height: f32,
  content: Content,
  annotations: Vec<PreparedAnnotation>,
  images: Vec<PreparedImageData>,
  fonts: BTreeSet<FontResource>,
  element_count: usize,
  annotation_count: usize,
}

fn take_ref(next_ref: &mut i32) -> Ref {
  let reference = Ref::new(*next_ref);
  *next_ref += 1;
  reference
}

fn write_document_info(
  pdf: &mut Chunk,
  info_ref: Ref,
  title: &str,
  metadata: Option<PdfMetadataInput>,
) {
  let metadata = metadata.unwrap_or(PdfMetadataInput {
    title: None,
    author: None,
    creator: None,
    producer: None,
    subject: None,
    keywords: None,
    trapped: None,
  });
  let mut info = pdf
    .indirect(info_ref)
    .start::<pdf_writer::writers::DocumentInfo>();

  info.title(TextStr(metadata.title.as_deref().unwrap_or(title)));
  if let Some(author) = metadata.author.as_deref() {
    info.author(TextStr(author));
  }
  if let Some(creator) = metadata.creator.as_deref() {
    info.creator(TextStr(creator));
  }
  info.producer(TextStr(
    metadata.producer.as_deref().unwrap_or("pdf-crab-js"),
  ));
  if let Some(subject) = metadata.subject.as_deref() {
    info.subject(TextStr(subject));
  }
  if let Some(keywords) = metadata.keywords {
    info.keywords(TextStr(&keywords.join(", ")));
  }
  if let Some(trapped) = metadata.trapped {
    info.trapped(if trapped {
      TrappingStatus::Trapped
    } else {
      TrappingStatus::NotTrapped
    });
  }
}

struct PreparedPage {
  reference: Ref,
  content_ref: Ref,
  width: f32,
  height: f32,
  content: Option<Vec<u8>>,
  annotations: Vec<PreparedAnnotation>,
  images: Vec<PreparedImageData>,
  fonts: BTreeSet<FontResource>,
}

struct PreparedAnnotation {
  reference: Ref,
  annotation: LinkAnnotation,
}

struct LinkAnnotation {
  page_ref: Ref,
  rect: Rect,
  url: String,
  color: Option<(f32, f32, f32)>,
}

fn prepare_annotation(
  annotation: PdfAnnotationInput,
  unit: Unit,
  page_height: f32,
  page_ref: Ref,
  path: &str,
) -> Result<LinkAnnotation> {
  match annotation.r#type.as_str() {
    "link" => prepare_link_annotation(annotation, unit, page_height, page_ref, path),
    annotation_type => Err(invalid_arg(format!(
      "{path}.type must be \"link\", received \"{annotation_type}\""
    ))),
  }
}

fn prepare_link_annotation(
  annotation: PdfAnnotationInput,
  unit: Unit,
  page_height: f32,
  page_ref: Ref,
  path: &str,
) -> Result<LinkAnnotation> {
  let x = unit.coordinate(required_f32(annotation.x, &format!("{path}.x"))?);
  let y = unit.coordinate(required_f32(annotation.y, &format!("{path}.y"))?);
  let width = unit.coordinate(required_positive_f32(
    annotation.width,
    &format!("{path}.width"),
  )?);
  let height = unit.coordinate(required_positive_f32(
    annotation.height,
    &format!("{path}.height"),
  )?);
  let url = annotation
    .url
    .ok_or_else(|| required(format!("{path}.url")))?;
  if url.trim().is_empty() {
    return Err(invalid_arg(format!("{path}.url must not be empty")));
  }
  let color = optional_color(annotation.color, &format!("{path}.color"))?
    .map(|color| (color.red, color.green, color.blue));

  Ok(LinkAnnotation {
    page_ref,
    rect: Rect::new(x, page_height - y - height, x + width, page_height - y),
    url,
    color,
  })
}

fn write_annotation(pdf: &mut Chunk, annotation_ref: Ref, annotation: &LinkAnnotation) {
  let mut writer = pdf.annotation(annotation_ref);
  writer.subtype(AnnotationType::Link);
  writer.rect(annotation.rect);
  writer.page(annotation.page_ref);
  writer.contents(TextStr("Link"));
  if let Some((red, green, blue)) = annotation.color {
    writer.color_rgb(red, green, blue);
  }
  writer
    .action()
    .action_type(ActionType::Uri)
    .uri(Str(annotation.url.as_bytes()));
  writer.border_style().width(0.0).style(BorderType::Solid);
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn incremental_serializer_emits_valid_xref_offsets() {
    let mut state = PdfDocumentState::new(Some("stream test".to_string()), None, None).unwrap();
    state
      .add_page(
        PdfPageInput {
          width: 120.0,
          height: 120.0,
          elements: None,
          annotations: None,
        },
        "pages[0]",
      )
      .unwrap();
    let mut serializer = state.finish_stream(false).unwrap();
    let mut bytes = Vec::new();
    while let Some(chunk) = serializer.next_chunk(7).unwrap() {
      bytes.extend(chunk);
    }

    assert!(bytes.starts_with(b"%PDF-1.7"));
    assert!(bytes.ends_with(b"%%EOF\n"));
    let text = String::from_utf8_lossy(&bytes);
    let startxref = bytes
      .windows(b"startxref\n".len())
      .rposition(|window| window == b"startxref\n")
      .unwrap();
    let xref = bytes[..startxref]
      .windows(b"xref\n".len())
      .rposition(|window| window == b"xref\n")
      .unwrap();
    let offset_start = startxref + b"startxref\n".len();
    let offset: usize = std::str::from_utf8(&bytes[offset_start..])
      .unwrap()
      .lines()
      .next()
      .unwrap()
      .parse()
      .unwrap();
    assert_eq!(offset, xref);
    assert!(text.contains("0000000000 65535 f"));
  }

  #[test]
  fn header_first_serializer_preserves_absolute_xref_offsets() {
    let mut state = PdfDocumentState::new(Some("header first".to_string()), None, None).unwrap();
    state
      .add_page(
        PdfPageInput {
          width: 120.0,
          height: 120.0,
          elements: None,
          annotations: None,
        },
        "pages[0]",
      )
      .unwrap();
    let mut serializer = state.finish_stream(true).unwrap();
    let mut bytes = PDF_HEADER.to_vec();
    while let Some(chunk) = serializer.next_chunk(5).unwrap() {
      bytes.extend(chunk);
    }

    let startxref = bytes
      .windows(b"startxref\n".len())
      .rposition(|window| window == b"startxref\n")
      .unwrap();
    let xref = bytes[..startxref]
      .windows(b"xref\n".len())
      .rposition(|window| window == b"xref\n")
      .unwrap();
    let offset_start = startxref + b"startxref\n".len();
    let offset: usize = std::str::from_utf8(&bytes[offset_start..])
      .unwrap()
      .lines()
      .next()
      .unwrap()
      .parse()
      .unwrap();
    assert_eq!(offset, xref);

    let catalog_offset = bytes
      .windows(b"1 0 obj".len())
      .position(|window| window == b"1 0 obj")
      .unwrap();
    assert!(
      String::from_utf8_lossy(&bytes[xref..]).contains(&format!("{catalog_offset:010} 00000 n"))
    );
  }

  #[test]
  fn cancellation_releases_pending_pages() {
    let mut state = PdfDocumentState::new(None, None, None).unwrap();
    state
      .add_page(
        PdfPageInput {
          width: 100.0,
          height: 100.0,
          elements: None,
          annotations: None,
        },
        "pages[0]",
      )
      .unwrap();
    let mut serializer = state.finish_stream(false).unwrap();
    serializer.cancel();
    assert!(serializer.next_chunk(64).unwrap().is_none());
  }
}

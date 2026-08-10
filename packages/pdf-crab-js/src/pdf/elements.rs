use std::collections::BTreeSet;

use napi::Result;
use pdf_writer::{Content, Name, Str};

use super::{
  color::{black, optional_color, RgbColor},
  font::{FontRegistry, FontResource, ShapedRun},
  image::decode_image,
  input::{PdfElementInput, PdfPointInput},
  unit::Unit,
  validation::{invalid_arg, optional_positive_f32, required, required_f32, required_positive_f32},
};

const DEFAULT_FONT_SIZE: f32 = 12.0;
const DEFAULT_STROKE_WIDTH: f32 = 1.0;

pub(super) struct AppendElementContext<'a> {
  pub(super) unit: Unit,
  pub(super) page_height: f32,
  pub(super) images: &'a mut Vec<PreparedImageData>,
  pub(super) fonts: &'a mut FontRegistry,
  pub(super) used_fonts: &'a mut BTreeSet<FontResource>,
  pub(super) next_ref: &'a mut i32,
}

pub(super) fn append_element(
  content: &mut Content,
  element: PdfElementInput,
  context: &mut AppendElementContext<'_>,
  path: &str,
) -> Result<()> {
  match element.r#type.as_str() {
    "text" => append_text(
      content,
      element,
      context.unit,
      context.page_height,
      context.fonts,
      context.used_fonts,
      path,
    ),
    "line" => append_line(content, element, context.unit, context.page_height, path),
    "rect" => append_rect(content, element, context.unit, context.page_height, path),
    "textBox" => append_text_box(
      content,
      element,
      context.unit,
      context.page_height,
      context.fonts,
      context.used_fonts,
      path,
    ),
    "polygon" => append_polygon(content, element, context.unit, context.page_height, path),
    "path" => append_path(content, element, context.unit, context.page_height, path),
    "image" => append_image(
      content,
      element,
      context.unit,
      context.page_height,
      context.images,
      context.next_ref,
      path,
    ),
    element_type => Err(invalid_arg(format!(
      "{path}.type must be one of \"text\", \"line\", \"rect\", \"textBox\", \"polygon\", \"path\", or \"image\", received \"{element_type}\""
    ))),
  }
}

pub(super) struct PreparedImageData {
  pub(super) image_ref: pdf_writer::Ref,
  pub(super) mask_ref: Option<pdf_writer::Ref>,
  pub(super) name: Vec<u8>,
  pub(super) decoded: super::image::DecodedImage,
}

fn take_ref(next_ref: &mut i32) -> pdf_writer::Ref {
  let reference = pdf_writer::Ref::new(*next_ref);
  *next_ref += 1;
  reference
}

fn append_text(
  content: &mut Content,
  element: PdfElementInput,
  unit: Unit,
  page_height: f32,
  fonts: &mut FontRegistry,
  used_fonts: &mut BTreeSet<FontResource>,
  path: &str,
) -> Result<()> {
  let text = element
    .text
    .ok_or_else(|| required(format!("{path}.text")))?;
  let x = unit.coordinate(required_f32(element.x, &format!("{path}.x"))?);
  let y = unit.coordinate(required_f32(element.y, &format!("{path}.y"))?);
  let font = element.font.unwrap_or_else(|| "Helvetica".to_string());
  let font_size = optional_positive_f32(element.font_size, &format!("{path}.fontSize"))?
    .unwrap_or(DEFAULT_FONT_SIZE);
  let fill = optional_color(element.fill, &format!("{path}.fill"))?.unwrap_or_else(black);
  let runs = fonts.shape_text(&font, &text, font_size, &format!("{path}.text"))?;
  used_fonts.extend(runs.iter().map(ShapedRun::resource));

  content.save_state();
  set_fill(content, fill);
  content.begin_text();
  let baseline = page_height - y - font_size * 0.8;
  content.set_text_matrix([1.0, 0.0, 0.0, 1.0, x, baseline]);
  show_runs(content, &runs, font_size, 0.0);
  content.end_text();
  content.restore_state();

  Ok(())
}

fn append_line(
  content: &mut Content,
  element: PdfElementInput,
  unit: Unit,
  page_height: f32,
  path: &str,
) -> Result<()> {
  let x1 = unit.coordinate(required_f32(element.x1, &format!("{path}.x1"))?);
  let y1 = page_height - unit.coordinate(required_f32(element.y1, &format!("{path}.y1"))?);
  let x2 = unit.coordinate(required_f32(element.x2, &format!("{path}.x2"))?);
  let y2 = page_height - unit.coordinate(required_f32(element.y2, &format!("{path}.y2"))?);
  let stroke = optional_color(element.stroke, &format!("{path}.stroke"))?.unwrap_or_else(black);
  let stroke_width = optional_positive_f32(element.stroke_width, &format!("{path}.strokeWidth"))?
    .unwrap_or(DEFAULT_STROKE_WIDTH);

  content.save_state();
  set_stroke(content, stroke);
  content.set_line_width(stroke_width);
  content.move_to(x1, y1);
  content.line_to(x2, y2);
  content.stroke();
  content.restore_state();

  Ok(())
}

fn append_rect(
  content: &mut Content,
  element: PdfElementInput,
  unit: Unit,
  page_height: f32,
  path: &str,
) -> Result<()> {
  let x = unit.coordinate(required_f32(element.x, &format!("{path}.x"))?);
  let top = unit.coordinate(required_f32(element.y, &format!("{path}.y"))?);
  let width = unit.coordinate(required_positive_f32(
    element.width,
    &format!("{path}.width"),
  )?);
  let height = unit.coordinate(required_positive_f32(
    element.height,
    &format!("{path}.height"),
  )?);
  let fill = optional_color(element.fill, &format!("{path}.fill"))?;
  let stroke = optional_color(element.stroke, &format!("{path}.stroke"))?;
  let stroke_width = optional_positive_f32(element.stroke_width, &format!("{path}.strokeWidth"))?
    .unwrap_or(DEFAULT_STROKE_WIDTH);

  content.save_state();
  if let Some(fill) = fill {
    set_fill(content, fill);
  }
  if let Some(stroke) = stroke.or_else(|| fill.is_none().then(black)) {
    set_stroke(content, stroke);
    content.set_line_width(stroke_width);
  }
  content.rect(x, page_height - top - height, width, height);
  paint_path(
    content,
    fill.is_some(),
    stroke.is_some() || fill.is_none(),
    false,
    Winding::NonZero,
  );
  content.restore_state();

  Ok(())
}

fn append_text_box(
  content: &mut Content,
  element: PdfElementInput,
  unit: Unit,
  page_height: f32,
  fonts: &mut FontRegistry,
  used_fonts: &mut BTreeSet<FontResource>,
  path: &str,
) -> Result<()> {
  let text = element
    .text
    .ok_or_else(|| required(format!("{path}.text")))?;
  let x = unit.coordinate(required_f32(element.x, &format!("{path}.x"))?);
  let y = unit.coordinate(required_f32(element.y, &format!("{path}.y"))?);
  let width = unit.coordinate(required_positive_f32(
    element.width,
    &format!("{path}.width"),
  )?);
  let height = element
    .height
    .map(|height| required_positive_f32(Some(height), &format!("{path}.height")))
    .transpose()?
    .map(|height| unit.coordinate(height));
  let font = element.font.unwrap_or_else(|| "Helvetica".to_string());
  let font_size = optional_positive_f32(element.font_size, &format!("{path}.fontSize"))?
    .unwrap_or(DEFAULT_FONT_SIZE);
  let line_height = optional_positive_f32(element.line_height, &format!("{path}.lineHeight"))?
    .unwrap_or(font_size * 1.2);
  let fill = optional_color(element.fill, &format!("{path}.fill"))?.unwrap_or_else(black);
  let align = parse_align(element.align.as_deref(), &format!("{path}.align"))?;
  let overflow = parse_overflow(element.overflow.as_deref(), &format!("{path}.overflow"))?;
  let max_lines = match (height, overflow) {
    (Some(height), TextOverflow::Clip | TextOverflow::Ellipsis | TextOverflow::Paginate) => {
      (height / line_height).floor().max(1.0) as usize
    }
    _ => usize::MAX,
  };
  let mut lines = match element.layout_lines {
    Some(lines) => lines,
    None => fonts.wrap_text(
      &font,
      &text,
      width,
      font_size,
      element.hyphenate.unwrap_or(false),
      &format!("{path}.text"),
    )?,
  };
  if matches!(overflow, TextOverflow::Ellipsis) && lines.len() > max_lines {
    lines.truncate(max_lines);
    if let Some(last) = lines.last_mut() {
      last.text = truncate_with_ellipsis(fonts, &font, &last.text, width, font_size, path)?;
      last.paragraph_end = true;
    }
  }

  content.save_state();
  set_fill(content, fill);
  if let Some(height) = height {
    if matches!(
      overflow,
      TextOverflow::Clip | TextOverflow::Ellipsis | TextOverflow::Paginate
    ) {
      content
        .rect(x, page_height - y - height, width, height)
        .clip_nonzero()
        .end_path();
    }
  }
  content.begin_text();
  content.set_leading(line_height);

  for (line_index, line) in lines.into_iter().take(max_lines).enumerate() {
    let runs = fonts.shape_text(&font, &line.text, font_size, &format!("{path}.text"))?;
    used_fonts.extend(runs.iter().map(ShapedRun::resource));
    let line_width = runs.iter().map(ShapedRun::width).sum::<f32>();
    let adjusted_x = match align {
      TextAlign::Left | TextAlign::Justify => x,
      TextAlign::Center => x + (width - line_width) / 2.0,
      TextAlign::Right => x + width - line_width,
    };
    let cursor_y = page_height - y - font_size * 0.8 - line_height * line_index as f32;
    content.set_text_matrix([1.0, 0.0, 0.0, 1.0, adjusted_x, cursor_y]);
    let spaces = line
      .text
      .chars()
      .filter(|character| *character == ' ')
      .count();
    let extra_word_spacing =
      if matches!(align, TextAlign::Justify) && !line.paragraph_end && spaces > 0 {
        (width - line_width).max(0.0) / spaces as f32
      } else {
        0.0
      };
    show_runs(content, &runs, font_size, extra_word_spacing);
  }

  content.end_text();
  content.restore_state();

  Ok(())
}

fn append_polygon(
  content: &mut Content,
  element: PdfElementInput,
  unit: Unit,
  page_height: f32,
  path: &str,
) -> Result<()> {
  let points = required_points(element.points, &format!("{path}.points"), unit, page_height)?;
  if points.len() < 3 {
    return Err(invalid_arg(format!(
      "{path}.points must contain at least 3 points"
    )));
  }

  let fill = optional_color(element.fill, &format!("{path}.fill"))?;
  let stroke = optional_color(element.stroke, &format!("{path}.stroke"))?;
  let stroke_width = optional_positive_f32(element.stroke_width, &format!("{path}.strokeWidth"))?
    .unwrap_or(DEFAULT_STROKE_WIDTH);
  let winding = parse_winding(element.winding.as_deref(), &format!("{path}.winding"))?;

  content.save_state();
  apply_shape_style(content, fill, stroke, stroke_width);
  append_points(content, &points, true);
  paint_path(content, fill.is_some(), stroke.is_some(), true, winding);
  content.restore_state();

  Ok(())
}

fn append_path(
  content: &mut Content,
  element: PdfElementInput,
  unit: Unit,
  page_height: f32,
  path: &str,
) -> Result<()> {
  let points = required_points(element.points, &format!("{path}.points"), unit, page_height)?;
  if points.len() < 2 {
    return Err(invalid_arg(format!(
      "{path}.points must contain at least 2 points"
    )));
  }

  let fill = optional_color(element.fill, &format!("{path}.fill"))?;
  let stroke = optional_color(element.stroke, &format!("{path}.stroke"))?;
  let stroke_width = optional_positive_f32(element.stroke_width, &format!("{path}.strokeWidth"))?
    .unwrap_or(DEFAULT_STROKE_WIDTH);
  let closed = element.closed.unwrap_or(false);
  let winding = parse_winding(element.winding.as_deref(), &format!("{path}.winding"))?;

  content.save_state();
  apply_shape_style(content, fill, stroke, stroke_width);
  append_points(content, &points, closed);
  paint_path(
    content,
    fill.is_some(),
    stroke.is_some() || fill.is_none(),
    closed,
    winding,
  );
  content.restore_state();

  Ok(())
}

fn append_image(
  content: &mut Content,
  element: PdfElementInput,
  unit: Unit,
  page_height: f32,
  images: &mut Vec<PreparedImageData>,
  next_ref: &mut i32,
  path: &str,
) -> Result<()> {
  let image_data = element
    .image_data
    .ok_or_else(|| required(format!("{path}.imageData")))?;
  let decoded = decode_image(&image_data)?;
  let x = unit.coordinate(required_f32(element.x, &format!("{path}.x"))?);
  let top = unit.coordinate(required_f32(element.y, &format!("{path}.y"))?);
  let natural_width = decoded.width as f32;
  let natural_height = decoded.height as f32;
  let width = element
    .width
    .map(|value| {
      required_positive_f32(Some(value), &format!("{path}.width"))
        .map(|value| unit.coordinate(value))
    })
    .transpose()?
    .unwrap_or(natural_width);
  let height = element
    .height
    .map(|value| {
      required_positive_f32(Some(value), &format!("{path}.height"))
        .map(|value| unit.coordinate(value))
    })
    .transpose()?
    .unwrap_or_else(|| width * natural_height / natural_width);

  if width <= 0.0 || height <= 0.0 {
    return Err(invalid_arg(format!(
      "{path}.width and {path}.height must be greater than 0"
    )));
  }

  let image_ref = take_ref(next_ref);
  let mask_ref = decoded.alpha.as_ref().map(|_| take_ref(next_ref));
  let name = format!("Im{}", images.len() + 1).into_bytes();

  content.save_state();
  content.transform([width, 0.0, 0.0, height, x, page_height - top - height]);
  content.x_object(pdf_writer::Name(name.as_slice()));
  content.restore_state();

  images.push(PreparedImageData {
    image_ref,
    mask_ref,
    name,
    decoded,
  });

  Ok(())
}

fn set_fill(content: &mut Content, color: RgbColor) {
  content.set_fill_rgb(color.red, color.green, color.blue);
}

fn set_stroke(content: &mut Content, color: RgbColor) {
  content.set_stroke_rgb(color.red, color.green, color.blue);
}

fn apply_shape_style(
  content: &mut Content,
  fill: Option<RgbColor>,
  stroke: Option<RgbColor>,
  stroke_width: f32,
) {
  if let Some(fill) = fill {
    set_fill(content, fill);
  }
  set_stroke(content, stroke.unwrap_or_else(black));
  content.set_line_width(stroke_width);
}

fn append_points(content: &mut Content, points: &[PdfPoint], closed: bool) {
  let [first, rest @ ..] = points else {
    return;
  };

  content.move_to(first.x, first.y);
  for point in rest {
    content.line_to(point.x, point.y);
  }
  if closed {
    content.close_path();
  }
}

fn paint_path(
  content: &mut Content,
  has_fill: bool,
  has_stroke: bool,
  closed: bool,
  winding: Winding,
) {
  match (has_fill, has_stroke, closed, winding) {
    (true, true, true, Winding::NonZero) => {
      content.close_fill_nonzero_and_stroke();
    }
    (true, true, true, Winding::EvenOdd) => {
      content.close_fill_even_odd_and_stroke();
    }
    (true, true, false, Winding::NonZero) => {
      content.fill_nonzero_and_stroke();
    }
    (true, true, false, Winding::EvenOdd) => {
      content.fill_even_odd_and_stroke();
    }
    (true, false, _, Winding::NonZero) => {
      content.fill_nonzero();
    }
    (true, false, _, Winding::EvenOdd) => {
      content.fill_even_odd();
    }
    (false, true, true, _) => {
      content.close_and_stroke();
    }
    (false, true, false, _) => {
      content.stroke();
    }
    (false, false, _, _) => {
      content.end_path();
    }
  }
}

#[derive(Clone, Copy)]
struct PdfPoint {
  x: f32,
  y: f32,
}

fn required_points(
  points: Option<Vec<PdfPointInput>>,
  path: &str,
  unit: Unit,
  page_height: f32,
) -> Result<Vec<PdfPoint>> {
  let points = points.ok_or_else(|| required(path))?;
  points
    .into_iter()
    .enumerate()
    .map(|(index, point)| {
      if point.bezier.unwrap_or(false) {
        return Err(invalid_arg(format!(
          "{path}[{index}].bezier is not supported by the pdf-writer phase"
        )));
      }

      Ok(PdfPoint {
        x: unit.coordinate(required_f32(Some(point.x), &format!("{path}[{index}].x"))?),
        y: page_height
          - unit.coordinate(required_f32(Some(point.y), &format!("{path}[{index}].y"))?),
      })
    })
    .collect()
}

#[derive(Clone, Copy)]
enum Winding {
  NonZero,
  EvenOdd,
}

fn parse_winding(value: Option<&str>, path: &str) -> Result<Winding> {
  match value.unwrap_or("nonZero") {
    "nonZero" => Ok(Winding::NonZero),
    "evenOdd" => Ok(Winding::EvenOdd),
    value => Err(invalid_arg(format!(
      "{path} must be \"nonZero\" or \"evenOdd\", received \"{value}\""
    ))),
  }
}

#[derive(Clone, Copy)]
enum TextAlign {
  Left,
  Center,
  Right,
  Justify,
}

#[derive(Clone, Copy)]
enum TextOverflow {
  Visible,
  Clip,
  Ellipsis,
  Paginate,
}

fn parse_align(value: Option<&str>, path: &str) -> Result<TextAlign> {
  match value.unwrap_or("left") {
    "left" => Ok(TextAlign::Left),
    "center" => Ok(TextAlign::Center),
    "right" => Ok(TextAlign::Right),
    "justify" => Ok(TextAlign::Justify),
    value => Err(invalid_arg(format!(
      "{path} must be one of \"left\", \"center\", \"right\", or \"justify\", received \"{value}\""
    ))),
  }
}

fn parse_overflow(value: Option<&str>, path: &str) -> Result<TextOverflow> {
  match value.unwrap_or("visible") {
    "visible" => Ok(TextOverflow::Visible),
    "clip" => Ok(TextOverflow::Clip),
    "ellipsis" => Ok(TextOverflow::Ellipsis),
    "paginate" => Ok(TextOverflow::Paginate),
    value => Err(invalid_arg(format!(
      "{path} must be one of \"visible\", \"clip\", \"ellipsis\", or \"paginate\", received \"{value}\""
    ))),
  }
}

fn truncate_with_ellipsis(
  fonts: &mut FontRegistry,
  font: &str,
  line: &str,
  max_width: f32,
  font_size: f32,
  path: &str,
) -> Result<String> {
  const ELLIPSIS: &str = "…";
  let mut value = line.to_string();
  loop {
    let candidate = format!("{value}{ELLIPSIS}");
    if fonts.measure_text(font, &candidate, font_size, &format!("{path}.text"))? <= max_width {
      return Ok(candidate);
    }
    if value.pop().is_none() {
      return Ok(ELLIPSIS.to_string());
    }
  }
}

fn show_runs(content: &mut Content, runs: &[ShapedRun], font_size: f32, extra_word_spacing: f32) {
  for run in runs {
    let resource = run.resource();
    content.set_font(Name(resource.name.as_slice()), font_size);
    match run {
      ShapedRun::Builtin { bytes, .. } => {
        if extra_word_spacing > 0.0 {
          content.set_word_spacing(extra_word_spacing);
        }
        content.show(Str(bytes));
        if extra_word_spacing > 0.0 {
          content.set_word_spacing(0.0);
        }
      }
      ShapedRun::Embedded { glyphs, .. } => {
        let mut positioned = content.show_positioned();
        let mut items = positioned.items();
        for glyph in glyphs {
          let bytes = glyph.cid.to_be_bytes();
          items.show(Str(&bytes));
          let extra = if glyph.is_space {
            extra_word_spacing / font_size * 1000.0
          } else {
            0.0
          };
          let adjustment = glyph.adjustment - extra;
          if adjustment.abs() > 0.001 {
            items.adjust(adjustment);
          }
        }
      }
    }
  }
}

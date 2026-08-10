use std::collections::{BTreeMap, BTreeSet};

use miniz_oxide::deflate::compress_to_vec_zlib;
use napi::Result;
use pdf_writer::{
  types::{CidFontType, FontFlags, SystemInfo, UnicodeCmap},
  writers::WMode,
  Chunk, Filter, Finish, Name, Rect, Ref, Str,
};
use rustybuzz::UnicodeBuffer;
use subsetter::GlyphRemapper;
use ttf_parser::{Face, GlyphId, Tag};
use unicode_linebreak::linebreaks;

use super::{
  font_metrics::{
    HELVETICA_BOLD_WIDTHS, HELVETICA_WIDTHS, TIMES_BOLD_ITALIC_WIDTHS, TIMES_BOLD_WIDTHS,
    TIMES_ITALIC_WIDTHS, TIMES_ROMAN_WIDTHS,
  },
  input::{PdfFontRegistrationInput, PdfTextLineInput},
  validation::invalid_arg,
};

const PDF_UNITS_PER_EM: f32 = 1000.0;
const IDENTITY_H: &[u8] = b"Identity-H";
const SYSTEM_INFO: SystemInfo<'static> = SystemInfo {
  registry: Str(b"Adobe"),
  ordering: Str(b"Identity"),
  supplement: 0,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum BuiltinFont {
  TimesRoman,
  TimesBold,
  TimesItalic,
  TimesBoldItalic,
  Helvetica,
  HelveticaBold,
  HelveticaOblique,
  HelveticaBoldOblique,
  Courier,
  CourierOblique,
  CourierBold,
  CourierBoldOblique,
  Symbol,
  ZapfDingbats,
}

impl BuiltinFont {
  pub(super) fn resource_name(self) -> Name<'static> {
    match self {
      Self::TimesRoman => Name(b"F1"),
      Self::TimesBold => Name(b"F2"),
      Self::TimesItalic => Name(b"F3"),
      Self::TimesBoldItalic => Name(b"F4"),
      Self::Helvetica => Name(b"F5"),
      Self::HelveticaBold => Name(b"F6"),
      Self::HelveticaOblique => Name(b"F7"),
      Self::HelveticaBoldOblique => Name(b"F8"),
      Self::Courier => Name(b"F9"),
      Self::CourierOblique => Name(b"F10"),
      Self::CourierBold => Name(b"F11"),
      Self::CourierBoldOblique => Name(b"F12"),
      Self::Symbol => Name(b"F13"),
      Self::ZapfDingbats => Name(b"F14"),
    }
  }

  pub(super) fn base_name(self) -> Name<'static> {
    match self {
      Self::TimesRoman => Name(b"Times-Roman"),
      Self::TimesBold => Name(b"Times-Bold"),
      Self::TimesItalic => Name(b"Times-Italic"),
      Self::TimesBoldItalic => Name(b"Times-BoldItalic"),
      Self::Helvetica => Name(b"Helvetica"),
      Self::HelveticaBold => Name(b"Helvetica-Bold"),
      Self::HelveticaOblique => Name(b"Helvetica-Oblique"),
      Self::HelveticaBoldOblique => Name(b"Helvetica-BoldOblique"),
      Self::Courier => Name(b"Courier"),
      Self::CourierOblique => Name(b"Courier-Oblique"),
      Self::CourierBold => Name(b"Courier-Bold"),
      Self::CourierBoldOblique => Name(b"Courier-BoldOblique"),
      Self::Symbol => Name(b"Symbol"),
      Self::ZapfDingbats => Name(b"ZapfDingbats"),
    }
  }

  pub(super) fn ref_number(self) -> i32 {
    match self {
      Self::TimesRoman => 4,
      Self::TimesBold => 5,
      Self::TimesItalic => 6,
      Self::TimesBoldItalic => 7,
      Self::Helvetica => 8,
      Self::HelveticaBold => 9,
      Self::HelveticaOblique => 10,
      Self::HelveticaBoldOblique => 11,
      Self::Courier => 12,
      Self::CourierOblique => 13,
      Self::CourierBold => 14,
      Self::CourierBoldOblique => 15,
      Self::Symbol => 16,
      Self::ZapfDingbats => 17,
    }
  }

  fn width(self, byte: u8) -> f32 {
    let index = byte as usize;
    let width = match self {
      Self::Helvetica | Self::HelveticaOblique => HELVETICA_WIDTHS[index],
      Self::HelveticaBold | Self::HelveticaBoldOblique => HELVETICA_BOLD_WIDTHS[index],
      Self::TimesRoman => TIMES_ROMAN_WIDTHS[index],
      Self::TimesBold => TIMES_BOLD_WIDTHS[index],
      Self::TimesItalic => TIMES_ITALIC_WIDTHS[index],
      Self::TimesBoldItalic => TIMES_BOLD_ITALIC_WIDTHS[index],
      Self::Courier | Self::CourierOblique | Self::CourierBold | Self::CourierBoldOblique => 600,
      Self::Symbol | Self::ZapfDingbats => 500,
    };
    width as f32
  }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct FontResource {
  pub(super) name: Vec<u8>,
  pub(super) ref_number: i32,
}

impl FontResource {
  fn builtin(font: BuiltinFont) -> Self {
    Self {
      name: font.resource_name().0.to_vec(),
      ref_number: font.ref_number(),
    }
  }
}

pub(super) enum ShapedRun {
  Builtin {
    font: BuiltinFont,
    bytes: Vec<u8>,
    width: f32,
  },
  Embedded {
    resource: FontResource,
    glyphs: Vec<ShapedGlyph>,
    width: f32,
  },
}

impl ShapedRun {
  pub(super) fn resource(&self) -> FontResource {
    match self {
      Self::Builtin { font, .. } => FontResource::builtin(*font),
      Self::Embedded { resource, .. } => resource.clone(),
    }
  }

  pub(super) fn width(&self) -> f32 {
    match self {
      Self::Builtin { width, .. } | Self::Embedded { width, .. } => *width,
    }
  }
}

pub(super) struct ShapedGlyph {
  pub(super) cid: u16,
  pub(super) adjustment: f32,
  pub(super) is_space: bool,
}

#[derive(Clone, Copy)]
struct FontRefs {
  root: Ref,
  cid: Ref,
  descriptor: Ref,
  cmap: Ref,
  data: Ref,
}

struct RegisteredFont {
  family: String,
  bytes: Vec<u8>,
  fallback: Option<String>,
  resource: FontResource,
  refs: FontRefs,
  remapper: GlyphRemapper,
  widths: Vec<f32>,
  cmap: BTreeMap<u16, String>,
  weight: Option<u16>,
  style: Option<String>,
  used: bool,
}

pub(super) struct PreparedFontObject {
  pub(super) reference: Ref,
  pub(super) bytes: Vec<u8>,
}

#[derive(Default)]
pub(super) struct FontRegistry {
  builtins: BTreeSet<BuiltinFont>,
  custom: Vec<RegisteredFont>,
  custom_by_family: BTreeMap<String, usize>,
}

impl FontRegistry {
  pub(super) fn register(
    &mut self,
    input: PdfFontRegistrationInput,
    next_ref: &mut i32,
  ) -> Result<()> {
    let family = input.family.trim().to_string();
    if family.is_empty() {
      return Err(invalid_arg("font family must not be empty"));
    }
    if try_parse_builtin_font(&family).is_some() || self.custom_by_family.contains_key(&family) {
      return Err(invalid_arg(format!(
        "font family \"{family}\" has already been registered"
      )));
    }
    if let Some(fallback) = input.fallback.as_deref() {
      if fallback == family {
        return Err(invalid_arg(format!(
          "font \"{family}\" cannot fall back to itself"
        )));
      }
      if try_parse_builtin_font(fallback).is_none() && !self.custom_by_family.contains_key(fallback)
      {
        return Err(invalid_arg(format!(
          "fallback font \"{fallback}\" must be registered first"
        )));
      }
    }

    let bytes = input.data.to_vec();
    let notdef_width = {
      let face = Face::parse(&bytes, 0)
        .map_err(|_| invalid_arg(format!("font \"{family}\" is not a valid TTF or OTF font")))?;
      if face
        .tables()
        .os2
        .is_some_and(|os2| !os2.is_outline_embedding_allowed())
      {
        return Err(invalid_arg(format!(
          "font \"{family}\" does not permit outline embedding"
        )));
      }
      face
        .glyph_hor_advance(GlyphId(0))
        .map(|width| to_pdf_units(width as f32, face.units_per_em()))
        .unwrap_or(0.0)
    };

    let index = self.custom.len();
    let refs = FontRefs {
      root: take_ref(next_ref),
      cid: take_ref(next_ref),
      descriptor: take_ref(next_ref),
      cmap: take_ref(next_ref),
      data: take_ref(next_ref),
    };
    let resource = FontResource {
      name: format!("FC{}", index + 1).into_bytes(),
      ref_number: refs.root.get(),
    };

    self.custom.push(RegisteredFont {
      family: family.clone(),
      bytes,
      fallback: input.fallback,
      resource,
      refs,
      remapper: GlyphRemapper::new(),
      widths: vec![notdef_width],
      cmap: BTreeMap::new(),
      weight: input.weight,
      style: input.style,
      used: false,
    });
    self.custom_by_family.insert(family, index);
    Ok(())
  }

  pub(super) fn shape_text(
    &mut self,
    font: &str,
    text: &str,
    font_size: f32,
    path: &str,
  ) -> Result<Vec<ShapedRun>> {
    self.shape_text_inner(font, text, font_size, path, 0)
  }

  pub(super) fn measure_text(
    &mut self,
    font: &str,
    text: &str,
    font_size: f32,
    path: &str,
  ) -> Result<f32> {
    Ok(
      self
        .shape_text(font, text, font_size, path)?
        .iter()
        .map(ShapedRun::width)
        .sum(),
    )
  }

  pub(super) fn wrap_text(
    &mut self,
    font: &str,
    text: &str,
    max_width: f32,
    font_size: f32,
    hyphenate: bool,
    path: &str,
  ) -> Result<Vec<PdfTextLineInput>> {
    let mut lines = Vec::new();
    for paragraph in text.split('\n') {
      let first_line = lines.len();
      self.wrap_paragraph(
        font, paragraph, max_width, font_size, hyphenate, path, &mut lines,
      )?;
      if lines.len() > first_line {
        lines.last_mut().unwrap().paragraph_end = true;
      }
    }
    if lines.is_empty() {
      lines.push(PdfTextLineInput {
        text: String::new(),
        paragraph_end: true,
      });
    }
    Ok(lines)
  }

  pub(super) fn used_builtins(&self) -> impl Iterator<Item = BuiltinFont> + '_ {
    self.builtins.iter().copied()
  }

  pub(super) fn custom_len(&self) -> usize {
    self.custom.len()
  }

  pub(super) fn custom_is_used(&self, index: usize) -> bool {
    self.custom.get(index).is_some_and(|font| font.used)
  }

  pub(super) fn prepare_custom_objects(&self, index: usize) -> Result<Vec<PreparedFontObject>> {
    let font = self
      .custom
      .get(index)
      .ok_or_else(|| invalid_arg("registered font index is out of bounds"))?;
    if !font.used {
      return Ok(Vec::new());
    }

    let subset = subsetter::subset(&font.bytes, 0, &font.remapper).map_err(|error| {
      invalid_arg(format!(
        "failed to subset font \"{}\": {error}",
        font.family
      ))
    })?;
    let original = Face::parse(&font.bytes, 0)
      .map_err(|_| invalid_arg(format!("font \"{}\" became invalid", font.family)))?;
    let subset_face = Face::parse(&subset, 0)
      .map_err(|_| invalid_arg(format!("font subset for \"{}\" is invalid", font.family)))?;
    let cff = subset_face.raw_face().table(Tag::from_bytes(b"CFF "));
    let is_cff = cff.is_some();
    let program = cff.unwrap_or(&subset);
    let base_name = subset_name(&font.family);

    let mut objects = Vec::with_capacity(5);

    let mut root = Chunk::new();
    root
      .type0_font(font.refs.root)
      .base_font(Name(base_name.as_bytes()))
      .encoding_predefined(Name(IDENTITY_H))
      .descendant_font(font.refs.cid)
      .to_unicode(font.refs.cmap);
    objects.push(prepared(font.refs.root, root));

    let mut cid_chunk = Chunk::new();
    let mut cid = cid_chunk.cid_font(font.refs.cid);
    cid
      .subtype(if is_cff {
        CidFontType::Type0
      } else {
        CidFontType::Type2
      })
      .base_font(Name(base_name.as_bytes()))
      .system_info(SYSTEM_INFO)
      .font_descriptor(font.refs.descriptor)
      .default_width(0.0);
    cid.widths().consecutive(0, font.widths.iter().copied());
    if !is_cff {
      cid.cid_to_gid_map_predefined(Name(b"Identity"));
    }
    cid.finish();
    objects.push(prepared(font.refs.cid, cid_chunk));

    let units_per_em = original.units_per_em();
    let scale = |value: f32| to_pdf_units(value, units_per_em);
    let bbox = original.global_bounding_box();
    let mut flags = FontFlags::SYMBOLIC;
    if original.is_monospaced() {
      flags.insert(FontFlags::FIXED_PITCH);
    }
    if original.is_italic() || font.style.as_deref().is_some_and(|style| style != "normal") {
      flags.insert(FontFlags::ITALIC);
    }
    if font.family.to_ascii_lowercase().contains("serif") {
      flags.insert(FontFlags::SERIF);
    }
    let ascent = scale(original.ascender() as f32);
    let descent = scale(original.descender() as f32);
    let cap_height = original
      .tables()
      .os2
      .and_then(|os2| os2.capital_height())
      .map(|value| scale(value as f32))
      .unwrap_or(ascent);
    let weight = font.weight.unwrap_or_else(|| original.weight().to_number());

    let mut descriptor_chunk = Chunk::new();
    let mut descriptor = descriptor_chunk.font_descriptor(font.refs.descriptor);
    descriptor
      .name(Name(base_name.as_bytes()))
      .family(Str(font.family.as_bytes()))
      .weight(weight)
      .flags(flags)
      .bbox(Rect::new(
        scale(bbox.x_min as f32),
        scale(bbox.y_min as f32),
        scale(bbox.x_max as f32),
        scale(bbox.y_max as f32),
      ))
      .italic_angle(original.italic_angle())
      .ascent(ascent)
      .descent(descent)
      .cap_height(cap_height)
      .stem_v(80.0);
    if is_cff {
      descriptor.font_file3(font.refs.data);
    } else {
      descriptor.font_file2(font.refs.data);
    }
    descriptor.finish();
    objects.push(prepared(font.refs.descriptor, descriptor_chunk));

    let mut cmap = UnicodeCmap::<u16>::new(Name(b"PdfCrabUnicode"), SYSTEM_INFO);
    for (&cid, text) in &font.cmap {
      if cid != 0 && !text.is_empty() {
        cmap.pair_with_multiple(cid, text.chars());
      }
    }
    let cmap = cmap.finish();
    let compressed_cmap = compress_to_vec_zlib(cmap.as_slice(), 6);
    let mut cmap_chunk = Chunk::new();
    {
      let mut writer = cmap_chunk.cmap(font.refs.cmap, &compressed_cmap);
      writer.filter(Filter::FlateDecode);
      writer.writing_mode(WMode::Horizontal);
    }
    objects.push(prepared(font.refs.cmap, cmap_chunk));

    let compressed_program = compress_to_vec_zlib(program, 6);
    let mut data_chunk = Chunk::new();
    {
      let mut stream = data_chunk.stream(font.refs.data, &compressed_program);
      stream
        .filter(Filter::FlateDecode)
        .pair(Name(b"Length1"), program.len() as i32);
      if is_cff {
        stream.pair(Name(b"Subtype"), Name(b"CIDFontType0C"));
      }
    }
    objects.push(prepared(font.refs.data, data_chunk));

    Ok(objects)
  }

  fn shape_text_inner(
    &mut self,
    font: &str,
    text: &str,
    font_size: f32,
    path: &str,
    depth: usize,
  ) -> Result<Vec<ShapedRun>> {
    if depth > self.custom.len() + 1 {
      return Err(invalid_arg(format!(
        "{path} contains a circular font fallback"
      )));
    }
    if let Some(builtin) = try_parse_builtin_font(font) {
      return self
        .shape_builtin(builtin, text, font_size, path)
        .map(|run| vec![run]);
    }
    let Some(&index) = self.custom_by_family.get(font) else {
      return Err(invalid_arg(format!(
        "{path} font \"{font}\" was not registered"
      )));
    };

    let (segments, fallback) = {
      let registered = &self.custom[index];
      let face = Face::parse(&registered.bytes, 0).map_err(|_| {
        invalid_arg(format!(
          "registered font \"{}\" is invalid",
          registered.family
        ))
      })?;
      let mut segments = Vec::<(bool, String)>::new();
      for character in text.chars() {
        if character == '\u{ad}' {
          continue;
        }
        let supported = face.glyph_index(character).is_some();
        if segments
          .last()
          .is_some_and(|(value, _)| *value == supported)
        {
          segments.last_mut().unwrap().1.push(character);
        } else {
          segments.push((supported, character.to_string()));
        }
      }
      (segments, registered.fallback.clone())
    };

    let mut runs = Vec::new();
    for (supported, segment) in segments {
      if supported {
        if !segment.is_empty() {
          runs.push(self.shape_custom(index, &segment, font_size, path)?);
        }
      } else if let Some(fallback) = fallback.as_deref() {
        if fallback == font {
          return Err(invalid_arg(format!(
            "{path} font \"{font}\" falls back to itself"
          )));
        }
        runs.extend(self.shape_text_inner(fallback, &segment, font_size, path, depth + 1)?);
      } else {
        let character = segment.chars().next().unwrap();
        return Err(missing_glyph(path, font, character));
      }
    }
    Ok(runs)
  }

  fn shape_builtin(
    &mut self,
    font: BuiltinFont,
    text: &str,
    font_size: f32,
    path: &str,
  ) -> Result<ShapedRun> {
    let mut bytes = Vec::with_capacity(text.len());
    let mut width = 0.0;
    for character in text.chars() {
      if character == '\u{ad}' {
        continue;
      }
      let byte = encode_win_ansi(character).ok_or_else(|| {
        invalid_arg(format!(
          "{path} contains unsupported glyph U+{:04X}; register a font with glyph coverage",
          character as u32
        ))
      })?;
      bytes.push(byte);
      width += font.width(byte) * font_size / PDF_UNITS_PER_EM;
    }
    self.builtins.insert(font);
    Ok(ShapedRun::Builtin { font, bytes, width })
  }

  fn shape_custom(
    &mut self,
    index: usize,
    text: &str,
    font_size: f32,
    path: &str,
  ) -> Result<ShapedRun> {
    struct RawGlyph {
      old_gid: u16,
      natural_width: f32,
      shaped_advance: f32,
      text: String,
      is_space: bool,
    }

    let raw = {
      let font = &self.custom[index];
      let face = rustybuzz::Face::from_slice(&font.bytes, 0)
        .ok_or_else(|| invalid_arg(format!("{path} font \"{}\" is invalid", font.family)))?;
      let mut buffer = UnicodeBuffer::new();
      buffer.push_str(text);
      buffer.guess_segment_properties();
      let shaped = rustybuzz::shape(&face, &[], buffer);
      let infos = shaped.glyph_infos();
      let positions = shaped.glyph_positions();
      let mut clusters = infos
        .iter()
        .map(|info| info.cluster as usize)
        .collect::<Vec<_>>();
      clusters.sort_unstable();
      clusters.dedup();
      clusters.push(text.len());
      let units_per_em = face.units_per_em() as f32;
      let mut raw = Vec::with_capacity(infos.len());
      for (info, position) in infos.iter().zip(positions) {
        let old_gid = info.glyph_id as u16;
        if old_gid == 0 {
          let start = info.cluster as usize;
          let character = text[start..].chars().next().unwrap_or('\u{fffd}');
          return Err(missing_glyph(path, &font.family, character));
        }
        let start = info.cluster as usize;
        let end = clusters
          .iter()
          .copied()
          .find(|cluster| *cluster > start)
          .unwrap_or(text.len());
        let cluster_text = text.get(start..end).unwrap_or_default().to_string();
        let natural_width = face
          .glyph_hor_advance(GlyphId(old_gid))
          .map(|width| width as f32 / units_per_em * PDF_UNITS_PER_EM)
          .unwrap_or(0.0);
        raw.push(RawGlyph {
          old_gid,
          natural_width,
          shaped_advance: position.x_advance as f32 / units_per_em * PDF_UNITS_PER_EM,
          is_space: cluster_text == " ",
          text: cluster_text,
        });
      }
      raw
    };

    let font = &mut self.custom[index];
    font.used = true;
    let mut glyphs = Vec::with_capacity(raw.len());
    let mut width = 0.0;
    for glyph in raw {
      let cid = font.remapper.remap(glyph.old_gid);
      if cid as usize >= font.widths.len() {
        font.widths.push(glyph.natural_width);
      }
      font.cmap.entry(cid).or_insert(glyph.text);
      width += glyph.shaped_advance * font_size / PDF_UNITS_PER_EM;
      glyphs.push(ShapedGlyph {
        cid,
        adjustment: glyph.natural_width - glyph.shaped_advance,
        is_space: glyph.is_space,
      });
    }

    Ok(ShapedRun::Embedded {
      resource: font.resource.clone(),
      glyphs,
      width,
    })
  }

  #[allow(clippy::too_many_arguments)]
  fn wrap_paragraph(
    &mut self,
    font: &str,
    paragraph: &str,
    max_width: f32,
    font_size: f32,
    hyphenate: bool,
    path: &str,
    lines: &mut Vec<PdfTextLineInput>,
  ) -> Result<()> {
    if paragraph.is_empty() {
      lines.push(PdfTextLineInput {
        text: String::new(),
        paragraph_end: false,
      });
      return Ok(());
    }

    let opportunities = linebreaks(paragraph)
      .map(|(index, _)| index)
      .collect::<Vec<_>>();
    let mut cursor = 0;
    let mut current = String::new();
    for end in opportunities {
      let piece = &paragraph[cursor..end];
      let candidate = format!("{current}{piece}");
      if self.measure_visible(font, &candidate, font_size, path)? <= max_width || current.is_empty()
      {
        current = candidate;
      } else {
        lines.push(PdfTextLineInput {
          text: display_line(&current, true),
          paragraph_end: false,
        });
        current = piece.trim_start().to_string();
      }

      while !current.is_empty()
        && self.measure_visible(font, &current, font_size, path)? > max_width
      {
        let (line, remainder) =
          self.break_overlong(font, &current, max_width, font_size, hyphenate, path)?;
        lines.push(PdfTextLineInput {
          text: line,
          paragraph_end: false,
        });
        current = remainder;
      }
      cursor = end;
    }

    if !current.is_empty() {
      lines.push(PdfTextLineInput {
        text: display_line(&current, false),
        paragraph_end: false,
      });
    }
    Ok(())
  }

  fn measure_visible(&mut self, font: &str, text: &str, font_size: f32, path: &str) -> Result<f32> {
    self.measure_text(font, &text.replace('\u{ad}', ""), font_size, path)
  }

  fn break_overlong(
    &mut self,
    font: &str,
    text: &str,
    max_width: f32,
    font_size: f32,
    hyphenate: bool,
    path: &str,
  ) -> Result<(String, String)> {
    let mut split = 0;
    for (index, character) in text.char_indices() {
      let end = index + character.len_utf8();
      let mut candidate = display_line(&text[..end], false);
      if hyphenate && end < text.len() {
        candidate.push('-');
      }
      if self.measure_text(font, &candidate, font_size, path)? > max_width {
        break;
      }
      split = end;
    }
    if split == 0 {
      split = text.chars().next().map_or(text.len(), char::len_utf8);
    }
    let mut line = display_line(&text[..split], false);
    if hyphenate && split < text.len() && !line.ends_with('-') {
      line.push('-');
    }
    Ok((line, text[split..].trim_start().to_string()))
  }
}

pub(super) fn try_parse_builtin_font(font: &str) -> Option<BuiltinFont> {
  let mut normalized = font.to_ascii_lowercase();
  normalized.retain(|character| !matches!(character, ' ' | '-' | '_'));
  match normalized.as_str() {
    "times" | "timesroman" | "timesnewroman" => Some(BuiltinFont::TimesRoman),
    "timesbold" | "timesnewromanbold" => Some(BuiltinFont::TimesBold),
    "timesitalic" | "timesnewromanitalic" => Some(BuiltinFont::TimesItalic),
    "timesbolditalic" | "timesitalicbold" | "timesnewromanbolditalic" => {
      Some(BuiltinFont::TimesBoldItalic)
    }
    "helvetica" | "arial" => Some(BuiltinFont::Helvetica),
    "helveticabold" | "arialbold" => Some(BuiltinFont::HelveticaBold),
    "helveticaoblique" | "helveticaitalic" | "arialitalic" => Some(BuiltinFont::HelveticaOblique),
    "helveticaboldoblique" | "helveticabolditalic" | "arialbolditalic" => {
      Some(BuiltinFont::HelveticaBoldOblique)
    }
    "courier" | "couriernew" => Some(BuiltinFont::Courier),
    "courieroblique" | "courieritalic" | "couriernewitalic" => Some(BuiltinFont::CourierOblique),
    "courierbold" | "couriernewbold" => Some(BuiltinFont::CourierBold),
    "courierboldoblique" | "courierbolditalic" | "couriernewbolditalic" => {
      Some(BuiltinFont::CourierBoldOblique)
    }
    "symbol" => Some(BuiltinFont::Symbol),
    "zapfdingbats" => Some(BuiltinFont::ZapfDingbats),
    _ => None,
  }
}

fn encode_win_ansi(character: char) -> Option<u8> {
  Some(match character {
    '\u{0}'..='\u{7f}' => character as u8,
    '\u{a0}'..='\u{ff}' => character as u8,
    '\u{20ac}' => 0x80,
    '\u{201a}' => 0x82,
    '\u{192}' => 0x83,
    '\u{201e}' => 0x84,
    '\u{2026}' => 0x85,
    '\u{2020}' => 0x86,
    '\u{2021}' => 0x87,
    '\u{2c6}' => 0x88,
    '\u{2030}' => 0x89,
    '\u{160}' => 0x8a,
    '\u{2039}' => 0x8b,
    '\u{152}' => 0x8c,
    '\u{17d}' => 0x8e,
    '\u{2018}' => 0x91,
    '\u{2019}' => 0x92,
    '\u{201c}' => 0x93,
    '\u{201d}' => 0x94,
    '\u{2022}' => 0x95,
    '\u{2013}' => 0x96,
    '\u{2014}' => 0x97,
    '\u{2dc}' => 0x98,
    '\u{2122}' => 0x99,
    '\u{161}' => 0x9a,
    '\u{203a}' => 0x9b,
    '\u{153}' => 0x9c,
    '\u{17e}' => 0x9e,
    '\u{178}' => 0x9f,
    _ => return None,
  })
}

fn display_line(value: &str, broken: bool) -> String {
  let trimmed = value.trim_end();
  let ends_with_soft_hyphen = trimmed.ends_with('\u{ad}');
  let mut visible = trimmed.replace('\u{ad}', "");
  if broken && ends_with_soft_hyphen {
    visible.push('-');
  }
  visible
}

fn subset_name(family: &str) -> String {
  let sanitized = family
    .chars()
    .filter(|character| character.is_ascii_alphanumeric())
    .take(100)
    .collect::<String>();
  format!(
    "CRABJS+{}",
    if sanitized.is_empty() {
      "Font"
    } else {
      &sanitized
    }
  )
}

fn to_pdf_units(value: f32, units_per_em: u16) -> f32 {
  value / units_per_em as f32 * PDF_UNITS_PER_EM
}

fn take_ref(next_ref: &mut i32) -> Ref {
  let reference = Ref::new(*next_ref);
  *next_ref += 1;
  reference
}

fn prepared(reference: Ref, chunk: Chunk) -> PreparedFontObject {
  PreparedFontObject {
    reference,
    bytes: chunk.as_bytes().to_vec(),
  }
}

fn missing_glyph(path: &str, font: &str, character: char) -> napi::Error {
  invalid_arg(format!(
    "{path} font \"{font}\" is missing glyph U+{:04X} ({character}) and has no usable fallback",
    character as u32
  ))
}

#[cfg(test)]
mod tests {
  use super::*;
  use napi::bindgen_prelude::Buffer;

  const TUFFY: &[u8] = include_bytes!("../../../../examples/html-to-pdf-crab-js/assets/Tuffy.ttf");

  fn register_tuffy(registry: &mut FontRegistry, family: &str, fallback: Option<&str>) {
    let mut next_ref = 18;
    registry
      .register(
        PdfFontRegistrationInput {
          family: family.to_string(),
          data: Buffer::from(TUFFY),
          fallback: fallback.map(str::to_string),
          weight: Some(400),
          style: Some("normal".to_string()),
        },
        &mut next_ref,
      )
      .unwrap();
  }

  #[test]
  fn builtin_measurement_uses_afm_widths() {
    let mut registry = FontRegistry::default();
    let narrow = registry
      .measure_text("Helvetica", "iiii", 12.0, "text")
      .unwrap();
    let wide = registry
      .measure_text("Helvetica", "WWWW", 12.0, "text")
      .unwrap();
    assert!(wide > narrow * 3.0);
  }

  #[test]
  fn custom_font_subsets_and_emits_unicode_mapping() {
    let mut registry = FontRegistry::default();
    register_tuffy(&mut registry, "Tuffy", Some("Helvetica"));
    let runs = registry
      .shape_text("Tuffy", "Ação\u{a0}final", 12.0, "text")
      .unwrap();
    assert_eq!(
      runs.len(),
      3,
      "NBSP should be delegated to the fallback run"
    );

    let objects = registry.prepare_custom_objects(0).unwrap();
    let bytes = objects
      .iter()
      .flat_map(|object| object.bytes.iter().copied())
      .collect::<Vec<_>>();
    let body = String::from_utf8_lossy(&bytes);
    assert!(body.contains("/Subtype /Type0"));
    assert!(body.contains("/ToUnicode"));
    assert!(body.contains("/FontFile2"));
    assert!(bytes.len() < TUFFY.len());
  }

  #[test]
  fn missing_custom_glyph_is_explicit_without_fallback() {
    let mut registry = FontRegistry::default();
    register_tuffy(&mut registry, "Tuffy", None);
    let error = registry
      .shape_text("Tuffy", "A\u{a0}B", 12.0, "text")
      .err()
      .unwrap();
    assert!(error.to_string().contains("U+00A0"));
  }
}

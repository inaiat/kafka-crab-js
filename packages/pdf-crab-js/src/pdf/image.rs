#![cfg_attr(test, allow(dead_code))]

use std::io::Cursor;

use image::{ColorType, ImageFormat, ImageReader};
use miniz_oxide::deflate::{compress_to_vec_zlib, CompressionLevel};
use napi::Result;
use pdf_writer::{Chunk, Filter, Finish, Ref};

use super::validation::invalid_arg;

#[derive(Clone, Copy)]
pub(super) enum ImageFilter {
  DctDecode,
  FlateDecode,
}

impl ImageFilter {
  fn to_pdf(self) -> Filter {
    match self {
      Self::DctDecode => Filter::DctDecode,
      Self::FlateDecode => Filter::FlateDecode,
    }
  }
}

pub(super) struct DecodedImage {
  pub(super) width: u32,
  pub(super) height: u32,
  pub(super) encoded: Vec<u8>,
  pub(super) filter: ImageFilter,
  pub(super) alpha: Option<Vec<u8>>,
}

pub(super) fn image_dimensions(data: &[u8]) -> Result<(u32, u32)> {
  let reader = ImageReader::new(Cursor::new(data))
    .with_guessed_format()
    .map_err(|error| {
      invalid_arg(format!(
        "unsupported image format; expected PNG or JPEG: {error}"
      ))
    })?;

  reader.into_dimensions().map_err(|error| {
    invalid_arg(format!(
      "unsupported image format; expected PNG or JPEG: {error}"
    ))
  })
}

pub(super) fn decode_image(data: &[u8]) -> Result<DecodedImage> {
  let format = image::guess_format(data).map_err(|error| {
    invalid_arg(format!(
      "unsupported image format; expected PNG or JPEG: {error}"
    ))
  })?;
  if !matches!(format, ImageFormat::Png | ImageFormat::Jpeg) {
    return Err(invalid_arg(
      "unsupported image format; expected PNG or JPEG",
    ));
  }

  let dynamic = image::load_from_memory_with_format(data, format)
    .map_err(|error| invalid_arg(format!("image could not be decoded: {error}")))?;
  let width = dynamic.width();
  let height = dynamic.height();

  if width == 0 || height == 0 {
    return Err(invalid_arg("image dimensions must be greater than 0"));
  }

  if format == ImageFormat::Jpeg && dynamic.color() == ColorType::Rgb8 {
    return Ok(DecodedImage {
      width,
      height,
      encoded: data.to_vec(),
      filter: ImageFilter::DctDecode,
      alpha: None,
    });
  }

  let rgb = dynamic.to_rgb8();
  let level = CompressionLevel::DefaultLevel as u8;
  let encoded = compress_to_vec_zlib(rgb.as_raw(), level);
  let alpha = dynamic.color().has_alpha().then(|| {
    let rgba = dynamic.to_rgba8();
    let values: Vec<u8> = rgba.pixels().map(|pixel| pixel[3]).collect();
    compress_to_vec_zlib(&values, level)
  });

  Ok(DecodedImage {
    width,
    height,
    encoded,
    filter: ImageFilter::FlateDecode,
    alpha,
  })
}

pub(super) fn write_image_xobject(
  pdf: &mut Chunk,
  image_ref: Ref,
  mask_ref: Option<Ref>,
  decoded: &DecodedImage,
) {
  let mut image = pdf.image_xobject(image_ref, &decoded.encoded);
  image.filter(decoded.filter.to_pdf());
  image.width(decoded.width as i32);
  image.height(decoded.height as i32);
  image.color_space().device_rgb();
  image.bits_per_component(8);
  if let Some(mask_ref) = mask_ref {
    image.s_mask(mask_ref);
  }
  image.finish();
}

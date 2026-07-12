use anyhow::{Context, Result};
use image::GrayImage;
use std::io::{self, Write};

const WECHAT_QR_MODULES: u32 = 41;
const WECHAT_QR_BORDER_PX: u32 = 30;
const WECHAT_QR_MODULE_PX: u32 = 10;
const TERMINAL_QR_QUIET_ZONE: u32 = 4;
const QR_DARK_THRESHOLD: u8 = 160;

pub(super) fn print(bytes: &[u8]) -> Result<()> {
    let image = image::load_from_memory(bytes)
        .context("failed to decode WeChat QR image")?
        .to_luma8();
    let modules = sample_modules(&image).context("failed to sample WeChat QR modules")?;
    print_modules(&modules)
}

fn print_modules(modules: &[bool]) -> Result<()> {
    let output_size = WECHAT_QR_MODULES + TERMINAL_QR_QUIET_ZONE * 2;

    println!();
    for y in (0..output_size).step_by(2) {
        for x in 0..output_size {
            let top_dark = rendered_module_dark(modules, x, y, output_size);
            let bottom_dark =
                y + 1 < output_size && rendered_module_dark(modules, x, y + 1, output_size);
            print_half_block(top_dark, bottom_dark, x, y);
        }
        println!("\x1b[0m");
    }
    println!("\x1b[0m");
    io::stdout().flush().context("failed to flush QR code")?;
    Ok(())
}

fn sample_modules(image: &GrayImage) -> Option<Vec<bool>> {
    let required_size = WECHAT_QR_BORDER_PX * 2 + WECHAT_QR_MODULES * WECHAT_QR_MODULE_PX;
    if image.width() < required_size || image.height() < required_size {
        return None;
    }

    let mut modules = Vec::with_capacity((WECHAT_QR_MODULES * WECHAT_QR_MODULES) as usize);
    for row in 0..WECHAT_QR_MODULES {
        for col in 0..WECHAT_QR_MODULES {
            let x = WECHAT_QR_BORDER_PX + col * WECHAT_QR_MODULE_PX + WECHAT_QR_MODULE_PX / 2;
            let y = WECHAT_QR_BORDER_PX + row * WECHAT_QR_MODULE_PX + WECHAT_QR_MODULE_PX / 2;
            modules.push(image.get_pixel(x, y)[0] < QR_DARK_THRESHOLD);
        }
    }

    Some(modules)
}

fn rendered_module_dark(modules: &[bool], x: u32, y: u32, output_size: u32) -> bool {
    if x < TERMINAL_QR_QUIET_ZONE
        || y < TERMINAL_QR_QUIET_ZONE
        || x >= output_size - TERMINAL_QR_QUIET_ZONE
        || y >= output_size - TERMINAL_QR_QUIET_ZONE
    {
        return false;
    }

    let x = x - TERMINAL_QR_QUIET_ZONE;
    let y = y - TERMINAL_QR_QUIET_ZONE;
    let index = (y * WECHAT_QR_MODULES + x) as usize;
    modules.get(index).copied().unwrap_or(false)
}

#[derive(Clone, Copy)]
struct Rgb {
    red: u8,
    green: u8,
    blue: u8,
}

impl Rgb {
    const fn new(red: u8, green: u8, blue: u8) -> Self {
        Self { red, green, blue }
    }
}

fn print_half_block(top_dark: bool, bottom_dark: bool, x: u32, y: u32) {
    let foreground = module_color(top_dark, x, y);
    let background = module_color(bottom_dark, x, y + 1);

    print!(
        "\x1b[38;2;{};{};{};48;2;{};{};{}m\u{2580}",
        foreground.red,
        foreground.green,
        foreground.blue,
        background.red,
        background.green,
        background.blue
    );
}

fn module_color(dark: bool, x: u32, y: u32) -> Rgb {
    if !dark {
        return Rgb::new(250, 248, 239);
    }

    const AURORA: [Rgb; 7] = [
        Rgb::new(239, 90, 91),
        Rgb::new(232, 119, 49),
        Rgb::new(225, 181, 64),
        Rgb::new(72, 164, 89),
        Rgb::new(24, 164, 174),
        Rgb::new(54, 111, 199),
        Rgb::new(239, 90, 91),
    ];

    let diagonal = (x + y).saturating_sub(TERMINAL_QR_QUIET_ZONE * 2);
    let span = (WECHAT_QR_MODULES - 1) * 2;
    let scaled = diagonal * ((AURORA.len() - 1) as u32) * 256 / span.max(1);
    let index = (scaled / 256).min((AURORA.len() - 1) as u32) as usize;
    let next_index = (index + 1).min(AURORA.len() - 1);
    let amount = smoothstep_byte((scaled % 256) as u8);
    let hue_shift = ((x * 13 + y * 7 + diagonal * 5) % 17) as i16 - 8;
    let shifted_amount = offset_byte(amount, hue_shift);
    let color = blend_rgb(AURORA[index], AURORA[next_index], shifted_amount);
    let shimmer = 82 + ((x * 17 + y * 11 + diagonal * 3) % 19) as u8;

    scale_rgb(color, shimmer)
}

fn blend_rgb(start: Rgb, end: Rgb, amount: u8) -> Rgb {
    let amount = u16::from(amount);
    let inverse = 255 - amount;

    Rgb::new(
        (((u16::from(start.red) * inverse) + (u16::from(end.red) * amount)) / 255) as u8,
        (((u16::from(start.green) * inverse) + (u16::from(end.green) * amount)) / 255) as u8,
        (((u16::from(start.blue) * inverse) + (u16::from(end.blue) * amount)) / 255) as u8,
    )
}

fn smoothstep_byte(value: u8) -> u8 {
    let value = u16::from(value);
    ((value * value * (765 - 2 * value)) / (255 * 255)) as u8
}

fn offset_byte(value: u8, offset: i16) -> u8 {
    (i16::from(value) + offset).clamp(0, 255) as u8
}

fn scale_rgb(color: Rgb, percent: u8) -> Rgb {
    let percent = u16::from(percent);

    Rgb::new(
        ((u16::from(color.red) * percent) / 100) as u8,
        ((u16::from(color.green) * percent) / 100) as u8,
        ((u16::from(color.blue) * percent) / 100) as u8,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sampling_preserves_outer_black_border() {
        let image_size = WECHAT_QR_BORDER_PX * 2 + WECHAT_QR_MODULES * WECHAT_QR_MODULE_PX;
        let mut image = GrayImage::from_pixel(image_size, image_size, image::Luma([255]));
        for row in 0..WECHAT_QR_MODULES {
            for col in 0..WECHAT_QR_MODULES {
                if row != 0
                    && row != WECHAT_QR_MODULES - 1
                    && col != 0
                    && col != WECHAT_QR_MODULES - 1
                {
                    continue;
                }

                let start_x = WECHAT_QR_BORDER_PX + col * WECHAT_QR_MODULE_PX;
                let start_y = WECHAT_QR_BORDER_PX + row * WECHAT_QR_MODULE_PX;
                for y in start_y..start_y + WECHAT_QR_MODULE_PX {
                    for x in start_x..start_x + WECHAT_QR_MODULE_PX {
                        image.put_pixel(x, y, image::Luma([0]));
                    }
                }
            }
        }
        let modules = sample_modules(&image).unwrap();
        let output_size = WECHAT_QR_MODULES + TERMINAL_QR_QUIET_ZONE * 2;

        assert!(!rendered_module_dark(&modules, 0, 0, output_size));
        assert!(rendered_module_dark(
            &modules,
            TERMINAL_QR_QUIET_ZONE,
            TERMINAL_QR_QUIET_ZONE,
            output_size
        ));
        assert!(rendered_module_dark(
            &modules,
            output_size - TERMINAL_QR_QUIET_ZONE - 1,
            TERMINAL_QR_QUIET_ZONE,
            output_size
        ));
        assert!(rendered_module_dark(
            &modules,
            TERMINAL_QR_QUIET_ZONE,
            output_size - TERMINAL_QR_QUIET_ZONE - 1,
            output_size
        ));
        assert!(!rendered_module_dark(
            &modules,
            TERMINAL_QR_QUIET_ZONE + 1,
            TERMINAL_QR_QUIET_ZONE + 1,
            output_size
        ));
    }
}

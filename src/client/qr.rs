use anyhow::{Context, Result};
use image::GrayImage;
use std::io::{self, Write};

const MODULES: u32 = 41;
const QUIET_ZONE: u32 = 4;

/// 微信当前返回 41×41 模块二维码；用半块字符将两行压缩到一行终端文本。
pub fn print(image_bytes: &[u8]) -> Result<()> {
    let image = image::load_from_memory(image_bytes)
        .context("failed to decode WeChat QR image")?
        .to_luma8();
    let modules = sample(&image).context("failed to detect WeChat QR modules")?;
    let output_size = MODULES + QUIET_ZONE * 2;
    println!();
    for y in (0..output_size).step_by(2) {
        for x in 0..output_size {
            let top_dark = rendered_module_dark(&modules, x, y, output_size);
            let bottom_dark =
                y + 1 < output_size && rendered_module_dark(&modules, x, y + 1, output_size);
            print_half_block(top_dark, bottom_dark, x, y);
        }
        println!("\x1b[0m");
    }
    println!("\x1b[0m");
    io::stdout().flush().context("failed to flush QR code")?;
    Ok(())
}

fn rendered_module_dark(modules: &[bool], x: u32, y: u32, output_size: u32) -> bool {
    if x < QUIET_ZONE
        || y < QUIET_ZONE
        || x >= output_size - QUIET_ZONE
        || y >= output_size - QUIET_ZONE
    {
        return false;
    }
    let index = ((y - QUIET_ZONE) * MODULES + (x - QUIET_ZONE)) as usize;
    modules.get(index).copied().unwrap_or(false)
}

#[derive(Clone, Copy)]
struct Rgb(u8, u8, u8);

fn print_half_block(top_dark: bool, bottom_dark: bool, x: u32, y: u32) {
    let foreground = module_color(top_dark, x, y);
    let background = module_color(bottom_dark, x, y + 1);
    print!(
        "\x1b[38;2;{};{};{};48;2;{};{};{}m▀",
        foreground.0, foreground.1, foreground.2, background.0, background.1, background.2
    );
}

fn module_color(dark: bool, x: u32, y: u32) -> Rgb {
    if !dark {
        return Rgb(250, 248, 239);
    }
    const AURORA: [Rgb; 7] = [
        Rgb(239, 90, 91),
        Rgb(232, 119, 49),
        Rgb(225, 181, 64),
        Rgb(72, 164, 89),
        Rgb(24, 164, 174),
        Rgb(54, 111, 199),
        Rgb(239, 90, 91),
    ];
    let diagonal = (x + y).saturating_sub(QUIET_ZONE * 2);
    let span = (MODULES - 1) * 2;
    let scaled = diagonal * ((AURORA.len() - 1) as u32) * 256 / span.max(1);
    let index = (scaled / 256).min((AURORA.len() - 1) as u32) as usize;
    let next = (index + 1).min(AURORA.len() - 1);
    let amount = smoothstep((scaled % 256) as u8);
    let color = blend(AURORA[index], AURORA[next], amount);
    let brightness = 82 + ((x * 17 + y * 11 + diagonal * 3) % 19) as u8;
    scale(color, brightness)
}

fn blend(start: Rgb, end: Rgb, amount: u8) -> Rgb {
    let amount = u16::from(amount);
    let inverse = 255 - amount;
    Rgb(
        ((u16::from(start.0) * inverse + u16::from(end.0) * amount) / 255) as u8,
        ((u16::from(start.1) * inverse + u16::from(end.1) * amount) / 255) as u8,
        ((u16::from(start.2) * inverse + u16::from(end.2) * amount) / 255) as u8,
    )
}

fn smoothstep(value: u8) -> u8 {
    let value = u32::from(value);
    ((value * value * (765 - 2 * value)) / (255 * 255)) as u8
}

fn scale(color: Rgb, percent: u8) -> Rgb {
    let percent = u16::from(percent);
    Rgb(
        (u16::from(color.0) * percent / 100) as u8,
        (u16::from(color.1) * percent / 100) as u8,
        (u16::from(color.2) * percent / 100) as u8,
    )
}

fn sample(image: &GrayImage) -> Option<Vec<bool>> {
    if image.width() < MODULES || image.height() < MODULES {
        return None;
    }
    // 实测微信图为 430px，四周各 10~30px 留白。先寻找暗色像素边界。
    let mut min_x = image.width();
    let mut min_y = image.height();
    let mut max_x = 0;
    let mut max_y = 0;
    for (x, y, pixel) in image.enumerate_pixels() {
        if pixel[0] < 160 {
            min_x = min_x.min(x);
            min_y = min_y.min(y);
            max_x = max_x.max(x);
            max_y = max_y.max(y);
        }
    }
    if min_x > max_x || min_y > max_y {
        return None;
    }
    let width = max_x - min_x + 1;
    let height = max_y - min_y + 1;
    if width < MODULES || height < MODULES {
        return None;
    }
    let mut modules = Vec::with_capacity((MODULES * MODULES) as usize);
    for y in 0..MODULES {
        for x in 0..MODULES {
            let sample_x = min_x + ((2 * x + 1) * width) / (2 * MODULES);
            let sample_y = min_y + ((2 * y + 1) * height) / (2 * MODULES);
            modules.push(image.get_pixel(sample_x.min(max_x), sample_y.min(max_y))[0] < 160);
        }
    }
    Some(modules)
}

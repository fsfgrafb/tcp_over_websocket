use anyhow::{Context, Result};
use image::GrayImage;

const MODULES: u32 = 41;

/// 微信当前返回 41×41 模块二维码；按图像暗色区域采样并输出终端块字符。
pub fn print(image_bytes: &[u8]) -> Result<()> {
    let image = image::load_from_memory(image_bytes)
        .context("无法解析微信二维码图片")?
        .to_luma8();
    let modules = sample(&image).context("无法识别微信二维码模块")?;
    println!();
    for y in 0..MODULES + 4 {
        for x in 0..MODULES + 4 {
            let dark = if x < 2 || y < 2 || x >= MODULES + 2 || y >= MODULES + 2 {
                false
            } else {
                modules[((y - 2) * MODULES + (x - 2)) as usize]
            };
            print!("{}", if dark { "██" } else { "  " });
        }
        println!();
    }
    println!();
    Ok(())
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

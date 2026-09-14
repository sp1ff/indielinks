// Copyright (C) 2026 Michael Herstine <sp1ff@pobox.com>
//
// This file is part of indielinks.
//
// indielinks is free software: you can redistribute it and/or modify it under the terms of the GNU
// General Public License as published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// indielinks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
// even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// General Public License for more details.
//
// You should have received a copy of the GNU General Public License along with indielinks.  If not,
// see <http://www.gnu.org/licenses/>.

#![cfg(target_arch = "wasm32")]

//! The indielinks visual theme.
//!
//! Thaw components and application-authored Tailwind classes share the CSS variables emitted by
//! this theme. Keeping the palette here prevents the two styling systems from drifting apart.

use std::collections::HashMap;

use thaw::Theme;

const SYSTEM_FONT: &str = "'Segoe UI', 'Segoe UI Web (West European)', ui-sans-serif, system-ui, \
                          -apple-system, BlinkMacSystemFont, Roboto, 'Helvetica Neue', sans-serif";

/// Construct the indielinks light theme.
pub fn light() -> Theme {
    let brand_colors = HashMap::from([
        (10, "#061333"),
        (20, "#0B1F52"),
        (30, "#102B70"),
        (40, "#16388F"),
        (50, "#1944AD"),
        (60, "#1B4DC4"),
        (70, "#1D4ED8"),
        (80, "#2563EB"),
        (90, "#3B73EE"),
        (100, "#5385F1"),
        (110, "#6D98F4"),
        (120, "#88AAF6"),
        (130, "#A5BDF8"),
        (140, "#C0D0FA"),
        (150, "#DCE5FC"),
        (160, "#F1F5FE"),
    ]);
    let mut theme = Theme::custom_light(&brand_colors);

    theme.common.set_font_family_base(SYSTEM_FONT.to_owned());
    theme.common.set_font_size_base_400("15px".to_owned());
    theme.common.set_border_radius_small("4px".to_owned());
    theme.common.set_border_radius_medium("4px".to_owned());
    theme.common.set_border_radius_large("6px".to_owned());
    theme.common.set_border_radius_x_large("8px".to_owned());
    theme.common.set_duration_ultra_fast("120ms".to_owned());
    theme.common.set_duration_faster("150ms".to_owned());
    theme.common.set_duration_normal("180ms".to_owned());
    theme.common.set_duration_gentle("220ms".to_owned());
    theme.common.set_duration_slow("250ms".to_owned());

    theme
        .color
        .set_color_neutral_background_1("#FFFFFF".to_owned());
    theme
        .color
        .set_color_neutral_background_1_hover("#F8FAFC".to_owned());
    theme
        .color
        .set_color_neutral_background_1_pressed("#F4F6F8".to_owned());
    theme
        .color
        .set_color_neutral_background_3("#F4F6F8".to_owned());
    theme
        .color
        .set_color_neutral_background_3_hover("#E9EDF2".to_owned());
    theme
        .color
        .set_color_neutral_background_3_pressed("#DEE4EB".to_owned());
    theme
        .color
        .set_color_neutral_background_4("#F8FAFC".to_owned());
    theme
        .color
        .set_color_neutral_background_4_hover("#F4F6F8".to_owned());
    theme
        .color
        .set_color_neutral_background_4_pressed("#E9EDF2".to_owned());

    theme
        .color
        .set_color_neutral_foreground_1("#172033".to_owned());
    theme
        .color
        .set_color_neutral_foreground_1_hover("#172033".to_owned());
    theme
        .color
        .set_color_neutral_foreground_1_pressed("#172033".to_owned());
    theme
        .color
        .set_color_neutral_foreground_2("#5F6B7A".to_owned());
    theme
        .color
        .set_color_neutral_foreground_2_hover("#465365".to_owned());
    theme
        .color
        .set_color_neutral_foreground_2_pressed("#344153".to_owned());
    theme
        .color
        .set_color_neutral_foreground_3("#5F6B7A".to_owned());
    theme
        .color
        .set_color_neutral_foreground_on_brand("#FFFFFF".to_owned());

    theme.color.set_color_neutral_stroke_1("#D8DEE8".to_owned());
    theme
        .color
        .set_color_neutral_stroke_1_hover("#BCC5D1".to_owned());
    theme
        .color
        .set_color_neutral_stroke_1_pressed("#9FAAB8".to_owned());
    theme.color.set_color_neutral_stroke_2("#D8DEE8".to_owned());
    theme
        .color
        .set_color_neutral_stroke_accessible("#5F6B7A".to_owned());
    theme
        .color
        .set_color_neutral_stroke_accessible_hover("#465365".to_owned());
    theme
        .color
        .set_color_neutral_stroke_accessible_pressed("#344153".to_owned());

    theme.color.set_color_brand_background("#2563EB".to_owned());
    theme
        .color
        .set_color_brand_background_hover("#1D4ED8".to_owned());
    theme
        .color
        .set_color_brand_background_pressed("#1D4ED8".to_owned());
    theme
        .color
        .set_color_brand_foreground_1("#2563EB".to_owned());
    theme
        .color
        .set_color_brand_foreground_2("#1D4ED8".to_owned());
    theme
        .color
        .set_color_brand_foreground_link("#2563EB".to_owned());
    theme
        .color
        .set_color_brand_foreground_link_hover("#1D4ED8".to_owned());
    theme
        .color
        .set_color_brand_foreground_link_pressed("#1D4ED8".to_owned());
    theme.color.set_color_brand_stroke_1("#2563EB".to_owned());
    theme.color.set_color_stroke_focus_2("#2563EB".to_owned());

    theme
        .color
        .set_color_neutral_shadow_ambient("rgb(23 32 51 / 12%)".to_owned());
    theme
        .color
        .set_color_neutral_shadow_key("rgb(23 32 51 / 12%)".to_owned());
    theme
        .color
        .set_shadow16("0 8px 24px rgb(23 32 51 / 12%)".to_owned());
    theme
        .color
        .set_shadow64("0 8px 24px rgb(23 32 51 / 12%)".to_owned());

    theme
}

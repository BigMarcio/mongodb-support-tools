"""Tests for Plotly theme helpers."""
import plotly.graph_objects as go

from lib.plot_theme import (
    apply_mi_theme,
    no_data_text_style,
    register_mi_template,
    section_label_style,
    theme_tokens,
    THEME_DARK,
    THEME_LIGHT,
)


class TestThemeTokens:
    def test_light_tokens(self):
        tokens = theme_tokens(dark=False)
        assert tokens["paper_bgcolor"] == THEME_LIGHT["paper_bgcolor"]

    def test_dark_tokens(self):
        tokens = theme_tokens(dark=True)
        assert tokens["paper_bgcolor"] == THEME_DARK["paper_bgcolor"]


class TestAnnotationStyles:
    def test_section_label_style_keys(self):
        style = section_label_style()
        assert style["showarrow"] is False
        assert "font" in style

    def test_no_data_text_style(self):
        style = no_data_text_style(dark=True)
        assert style["size"] == 30


class TestApplyMiTheme:
    def test_apply_mi_theme_updates_layout(self):
        fig = go.Figure(data=[go.Scatter(x=[1, 2], y=[1, 2])])
        apply_mi_theme(fig, title="Test", height=200, width=400)
        assert fig.layout.title.text == "Test"
        assert fig.layout.height == 200

    def test_register_mi_template(self):
        register_mi_template()
        import plotly.io as pio

        assert "mongodb_insights" in pio.templates

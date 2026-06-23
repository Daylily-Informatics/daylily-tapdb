from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JS_PATH = ROOT / "daylily_tapdb" / "gui" / "static" / "js" / "lsmc-ui.js"
CSS_PATH = ROOT / "daylily_tapdb" / "gui" / "static" / "css" / "tapdb-gui.css"


def test_lsmc_theme_script_preserves_existing_and_new_themes() -> None:
    script = JS_PATH.read_text()

    for theme in ("original", "light", "dark", "cbf", "ssf", "viridis", "viridis-dark"):
        assert f'"{theme}"' in script

    assert 'cbf: "CBF"' in script
    assert 'ssf: "S.SF"' in script
    assert 'viridis: "Viridis"' in script
    assert '"viridis-dark": "Viridis Dark"' in script


def test_lsmc_theme_script_uses_explicit_global_and_service_storage() -> None:
    script = JS_PATH.read_text()

    assert 'const globalStorageKey = "lsmc.ui.theme";' in script
    assert 'const modeStoragePrefix = "lsmc.ui.theme.mode.";' in script
    assert 'const serviceStoragePrefix = "lsmc.ui.theme.service.";' in script
    assert (
        "window.localStorage.setItem(isGlobalThemeMode() ? globalStorageKey : serviceThemeKey(), value);"
        in script
    )


def test_lsmc_theme_css_defines_new_skin_selectors() -> None:
    styles = CSS_PATH.read_text()

    for theme in ("ssf", "viridis", "viridis-dark"):
        assert f'html[data-theme="{theme}"]' in styles

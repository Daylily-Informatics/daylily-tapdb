(function () {
  const themes = [
    "original",
    "light",
    "dark",
    "cbf",
    "ssf",
    "viridis",
    "viridis-dark",
  ];
  const themeLabels = {
    cbf: "CBF",
    ssf: "S.SF",
    viridis: "Viridis",
    "viridis-dark": "Viridis Dark",
  };
  const globalStorageKey = "lsmc.ui.theme";
  const modeStoragePrefix = "lsmc.ui.theme.mode.";
  const serviceStoragePrefix = "lsmc.ui.theme.service.";
  const service = document.documentElement.dataset.lsmcService || "tapdb";
  const serviceDefaults = {
    "kahlo": "dark",
    "bloom": "dark",
    "dewey": "dark",
    "qeo": "dark",
    "ursa": "original",
    "atlas": "original",
    "zebra-day": "original",
    "tapdb": "original",
  };

  function defaultTheme() {
    return serviceDefaults[service] || "original";
  }

  function serviceModeKey() {
    return `${modeStoragePrefix}${service}`;
  }

  function serviceThemeKey() {
    return `${serviceStoragePrefix}${service}`;
  }

  function isGlobalThemeMode() {
    return window.localStorage.getItem(serviceModeKey()) !== "service";
  }

  function currentTheme() {
    if (!isGlobalThemeMode()) {
      const serviceTheme = window.localStorage.getItem(serviceThemeKey());
      if (themes.includes(serviceTheme)) return serviceTheme;
    }
    const stored = window.localStorage.getItem(globalStorageKey);
    return themes.includes(stored) ? stored : defaultTheme();
  }

  function applyTheme(theme) {
    const value = themes.includes(theme) ? theme : defaultTheme();
    document.documentElement.dataset.theme = value;
    window.localStorage.setItem(isGlobalThemeMode() ? globalStorageKey : serviceThemeKey(), value);
  }

  function setThemeMode(globalMode) {
    window.localStorage.setItem(serviceModeKey(), globalMode ? "global" : "service");
  }

  function commandForPage() {
    if (location.pathname.includes("/search")) return "tapdb search --help";
    if (location.pathname.includes("/templates")) return "tapdb db templates --help";
    if (location.pathname.includes("/metrics")) return "tapdb ui metrics --help";
    if (location.pathname.includes("/graph")) return "tapdb dag --help";
    return `No CLI equivalent for tapdb ${location.pathname}`;
  }

  function initThemeControl() {
    const wrap = document.createElement("div");
    wrap.className = "lsmc-theme-control";
    const label = document.createElement("label");
    label.textContent = "Theme";
    const select = document.createElement("select");
    for (const theme of themes) select.appendChild(new Option(themeLabels[theme] || theme, theme));
    select.value = currentTheme();
    label.appendChild(select);
    const modeLabel = document.createElement("label");
    modeLabel.className = "lsmc-theme-global";
    const globalCheckbox = document.createElement("input");
    globalCheckbox.type = "checkbox";
    globalCheckbox.checked = isGlobalThemeMode();
    modeLabel.append(globalCheckbox, document.createTextNode("Global"));
    select.addEventListener("change", () => applyTheme(select.value));
    globalCheckbox.addEventListener("change", () => {
      setThemeMode(globalCheckbox.checked);
      select.value = currentTheme();
      applyTheme(select.value);
    });
    wrap.append(label, modeLabel);
    document.body.appendChild(wrap);
  }

  function initActionHelp() {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "lsmc-action-help-button";
    button.textContent = "?";
    const panel = document.createElement("aside");
    panel.className = "lsmc-action-help-panel";
    panel.hidden = true;
    panel.innerHTML = '<strong>Action Help</strong><pre></pre><button type="button">Copy</button>';
    const output = panel.querySelector("pre");
    button.addEventListener("click", () => {
      panel.hidden = !panel.hidden;
      output.textContent = commandForPage();
    });
    panel.querySelector("button").addEventListener("click", () => navigator.clipboard?.writeText(output.textContent || ""));
    document.body.append(button, panel);
  }

  applyTheme(currentTheme());
  document.addEventListener("DOMContentLoaded", () => {
    initThemeControl();
    initActionHelp();
  });
})();

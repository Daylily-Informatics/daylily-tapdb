(function () {
  function kind(value) {
    if (value === null) return "null";
    if (Array.isArray(value)) return "array";
    return typeof value === "number" ? "number" : typeof value;
  }

  function defaultValue(type) {
    if (type === "object") return {};
    if (type === "array") return [];
    if (type === "number") return 0;
    if (type === "boolean") return false;
    if (type === "null") return null;
    return "";
  }

  function makeButton(label, className) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = className || "secondary";
    button.textContent = label;
    return button;
  }

  function makeTypeSelect(value) {
    const select = document.createElement("select");
    for (const optionValue of ["object", "array", "string", "number", "boolean", "null"]) {
      const option = document.createElement("option");
      option.value = optionValue;
      option.textContent = optionValue;
      select.appendChild(option);
    }
    select.value = kind(value);
    return select;
  }

  function parseNumber(text) {
    const trimmed = String(text || "").trim();
    if (!trimmed) return 0;
    const parsed = Number(trimmed);
    if (!Number.isFinite(parsed)) {
      throw new Error("Number fields must contain a valid integer or decimal.");
    }
    return parsed;
  }

  function nextObjectKey(objectValue) {
    let index = Object.keys(objectValue).length + 1;
    while (Object.prototype.hasOwnProperty.call(objectValue, `field_${index}`)) index += 1;
    return `field_${index}`;
  }

  function initEditor(textarea) {
    let state;
    const wrapper = document.createElement("div");
    wrapper.className = "tapdb-json-editor";
    wrapper.dataset.tapdbJsonEditor = "ready";

    const title = document.createElement("div");
    title.className = "tapdb-json-editor-title";
    title.textContent = textarea.dataset.jsonEditorLabel || "JSON editor";
    const root = document.createElement("div");
    root.className = "tapdb-json-editor-root";
    const error = document.createElement("div");
    error.className = "tapdb-json-editor-error";
    error.hidden = true;
    wrapper.append(title, root, error);

    textarea.before(wrapper);
    textarea.hidden = true;
    textarea.setAttribute("aria-hidden", "true");
    textarea.classList.add("tapdb-json-source");

    function setError(message) {
      if (message) {
        error.textContent = message;
        error.hidden = false;
        wrapper.dataset.valid = "false";
      } else {
        error.textContent = "";
        error.hidden = true;
        wrapper.dataset.valid = "true";
      }
    }

    function sync() {
      try {
        textarea.value = JSON.stringify(state, null, 2);
        setError("");
        return true;
      } catch (exc) {
        setError(exc.message || String(exc));
        return false;
      }
    }

    function rerender() {
      root.replaceChildren();
      renderNode(root, state, (next) => {
        state = next;
        sync();
        rerender();
      });
      sync();
    }

    function renderNode(parent, value, setValue) {
      const node = document.createElement("div");
      node.className = `tapdb-json-node tapdb-json-node-${kind(value)}`;
      const toolbar = document.createElement("div");
      toolbar.className = "tapdb-json-node-toolbar";
      const select = makeTypeSelect(value);
      select.addEventListener("change", () => setValue(defaultValue(select.value)));
      toolbar.append(select);
      node.append(toolbar);

      if (kind(value) === "object") {
        renderObject(node, value, setValue);
      } else if (kind(value) === "array") {
        renderArray(node, value, setValue);
      } else {
        renderPrimitive(node, value, setValue);
      }
      parent.appendChild(node);
    }

    function renderObject(parent, objectValue, setValue) {
      const rows = document.createElement("div");
      rows.className = "tapdb-json-children";
      for (const key of Object.keys(objectValue)) {
        const row = document.createElement("div");
        row.className = "tapdb-json-row";

        const keyInput = document.createElement("input");
        keyInput.className = "tapdb-json-key";
        keyInput.value = key;
        keyInput.addEventListener("change", () => {
          const newKey = keyInput.value.trim();
          if (!newKey) {
            setError("Object keys cannot be empty.");
            keyInput.value = key;
            return;
          }
          if (newKey === key) return;
          if (Object.prototype.hasOwnProperty.call(objectValue, newKey)) {
            setError(`Object key already exists: ${newKey}`);
            keyInput.value = key;
            return;
          }
          objectValue[newKey] = objectValue[key];
          delete objectValue[key];
          setValue(objectValue);
          sync();
          rerender();
        });

        const valueWrap = document.createElement("div");
        valueWrap.className = "tapdb-json-value";
        renderNode(valueWrap, objectValue[key], (next) => {
          objectValue[key] = next;
          setValue(objectValue);
          sync();
        });

        const remove = makeButton("Remove", "secondary danger");
        remove.addEventListener("click", () => {
          delete objectValue[key];
          setValue(objectValue);
          rerender();
        });
        row.append(keyInput, valueWrap, remove);
        rows.appendChild(row);
      }
      const add = makeButton("Add key", "secondary");
      add.addEventListener("click", () => {
        objectValue[nextObjectKey(objectValue)] = "";
        setValue(objectValue);
        rerender();
      });
      parent.append(rows, add);
    }

    function renderArray(parent, arrayValue, setValue) {
      const rows = document.createElement("div");
      rows.className = "tapdb-json-children";
      arrayValue.forEach((item, index) => {
        const row = document.createElement("div");
        row.className = "tapdb-json-row";
        const label = document.createElement("span");
        label.className = "tapdb-json-index";
        label.textContent = `[${index}]`;
        const valueWrap = document.createElement("div");
        valueWrap.className = "tapdb-json-value";
        renderNode(valueWrap, item, (next) => {
          arrayValue[index] = next;
          setValue(arrayValue);
          sync();
        });
        const remove = makeButton("Remove", "secondary danger");
        remove.addEventListener("click", () => {
          arrayValue.splice(index, 1);
          setValue(arrayValue);
          rerender();
        });
        row.append(label, valueWrap, remove);
        rows.appendChild(row);
      });
      const add = makeButton("Add item", "secondary");
      add.addEventListener("click", () => {
        arrayValue.push("");
        setValue(arrayValue);
        rerender();
      });
      parent.append(rows, add);
    }

    function renderPrimitive(parent, value, setValue) {
      const type = kind(value);
      if (type === "boolean") {
        const select = document.createElement("select");
        for (const boolValue of ["true", "false"]) {
          const option = document.createElement("option");
          option.value = boolValue;
          option.textContent = boolValue;
          select.appendChild(option);
        }
        select.value = value ? "true" : "false";
        select.addEventListener("change", () => {
          setValue(select.value === "true");
          sync();
        });
        parent.appendChild(select);
        return;
      }
      if (type === "null") {
        const marker = document.createElement("span");
        marker.className = "tapdb-json-null";
        marker.textContent = "null";
        parent.appendChild(marker);
        return;
      }
      const input = document.createElement("input");
      input.className = "tapdb-json-primitive";
      input.type = type === "number" ? "number" : "text";
      input.value = type === "number" ? String(value) : String(value || "");
      input.addEventListener("input", () => {
        try {
          setValue(type === "number" ? parseNumber(input.value) : input.value);
          sync();
        } catch (exc) {
          setError(exc.message || String(exc));
        }
      });
      parent.appendChild(input);
    }

    try {
      state = JSON.parse(textarea.value || "{}");
      rerender();
    } catch (exc) {
      setError(`Initial JSON is invalid: ${exc.message || String(exc)}`);
    }

    const form = textarea.closest("form");
    if (form) {
      form.addEventListener("submit", (event) => {
        if (!sync() || wrapper.dataset.valid !== "true") {
          event.preventDefault();
          event.stopPropagation();
        }
      });
    }
  }

  function initAll() {
    document
      .querySelectorAll("textarea[data-tapdb-json-editor]")
      .forEach((textarea) => initEditor(textarea));
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initAll);
  } else {
    initAll();
  }
})();

(function () {
  function parseJson(text) {
    const raw = String(text || "").trim();
    return raw ? JSON.parse(raw) : {};
  }

  function formatJson(value) {
    return JSON.stringify(value, null, 2);
  }

  function setError(shell, message) {
    const error = shell.querySelector("[data-tapdb-json-editor-error]");
    if (!error) return;
    if (message) {
      error.textContent = message;
      error.hidden = false;
      shell.dataset.valid = "false";
    } else {
      error.textContent = "";
      error.hidden = true;
      shell.dataset.valid = "true";
    }
  }

  function initEditor(textarea) {
    if (textarea.dataset.tapdbJsonEditorReady === "true") return;
    textarea.dataset.tapdbJsonEditorReady = "true";

    const shell = document.createElement("div");
    shell.className = "tapdb-json-editor";
    shell.dataset.testid = "tapdb-json-editor";
    shell.dataset.valid = "true";

    const title = document.createElement("div");
    title.className = "tapdb-json-editor-title";
    title.textContent = textarea.dataset.jsonEditorLabel || "JSON editor";

    const widget = document.createElement("div");
    widget.className = "tapdb-json-editor-widget";
    widget.dataset.testid = "tapdb-json-editor-widget";

    const error = document.createElement("div");
    error.className = "tapdb-json-editor-error";
    error.dataset.tapdbJsonEditorError = "true";
    error.hidden = true;

    shell.append(title, widget, error);
    textarea.before(shell);

    if (!window.JSONEditor) {
      shell.dataset.valid = "false";
      setError(
        shell,
        "JSONEditor failed to load. Check network access to the pinned jsoneditor CDN asset."
      );
      return;
    }

    let lastText = textarea.value || "{}";
    let editor;
    const options = {
      mode: "text",
      modes: ["text", "tree", "form", "view"],
      mainMenuBar: true,
      navigationBar: true,
      statusBar: true,
      indentation: 2,
      onChangeText(text) {
        lastText = text;
        textarea.value = text;
        try {
          parseJson(text);
          setError(shell, "");
        } catch (exc) {
          setError(shell, exc.message || String(exc));
        }
      },
    };

    try {
      editor = new window.JSONEditor(widget, options);
      editor.setText(lastText || "{}");
      parseJson(editor.getText());
      textarea.value = editor.getText();
      textarea.hidden = true;
      textarea.setAttribute("aria-hidden", "true");
      textarea.classList.add("tapdb-json-source");
      setError(shell, "");
    } catch (exc) {
      setError(shell, exc.message || String(exc));
    }

    function syncToTextarea({ requireValid }) {
      if (!editor) return true;
      const text = editor.getText();
      lastText = text;
      textarea.value = text;
      if (!requireValid) return true;
      try {
        const parsed = parseJson(text);
        textarea.value = formatJson(parsed);
        editor.setText(textarea.value);
        setError(shell, "");
        return true;
      } catch (exc) {
        setError(shell, exc.message || String(exc));
        return false;
      }
    }

    function setValue(value) {
      if (!editor) return;
      const nextText = typeof value === "string" ? value : formatJson(value);
      lastText = nextText;
      textarea.value = nextText;
      editor.setText(nextText);
      try {
        parseJson(nextText);
        setError(shell, "");
      } catch (exc) {
        setError(shell, exc.message || String(exc));
      }
    }

    textarea.tapdbJsonEditor = {
      editor,
      shell,
      setValue,
      getText() {
        return editor ? editor.getText() : lastText;
      },
      syncToTextarea,
    };

    textarea.addEventListener("tapdb-json-editor:set", (event) => {
      setValue(event.detail && Object.prototype.hasOwnProperty.call(event.detail, "value")
        ? event.detail.value
        : textarea.value);
    });

    const form = textarea.form;
    if (form) {
      form.addEventListener("submit", (event) => {
        if (!syncToTextarea({ requireValid: true })) {
          event.preventDefault();
          event.stopPropagation();
          shell.scrollIntoView({ block: "center" });
        }
      });
    }
  }

  window.TapdbJsonEditor = {
    init(root) {
      const scope = root || document;
      scope.querySelectorAll("textarea[data-tapdb-json-editor]").forEach(initEditor);
    },
    refresh(textarea) {
      if (!textarea || !textarea.tapdbJsonEditor) return;
      textarea.tapdbJsonEditor.setValue(textarea.value);
    },
    setValue(textarea, value) {
      if (!textarea || !textarea.tapdbJsonEditor) return;
      textarea.tapdbJsonEditor.setValue(value);
    },
  };

  document.addEventListener("DOMContentLoaded", () => window.TapdbJsonEditor.init());
})();

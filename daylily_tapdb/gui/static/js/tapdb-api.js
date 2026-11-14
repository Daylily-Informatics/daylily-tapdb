/* Canonical GUI mutations use the same published JSON APIs as other clients. */
(() => {
  async function request(url, payload, method = "POST") {
    const response = await fetch(url, {
      method,
      credentials: "same-origin",
      headers: {"Content-Type": "application/json", "Accept": "application/json"},
      body: JSON.stringify(payload),
    });
    const text = await response.text();
    let result;
    try {
      result = JSON.parse(text);
    } catch (_) {
      throw new Error(`The API returned HTTP ${response.status} without a JSON result.`);
    }
    if (!response.ok) {
      const detail = result.detail;
      const error = new Error(typeof detail === "string" ? detail : JSON.stringify(detail ?? result));
      error.status = response.status;
      error.detail = detail;
      throw error;
    }
    return result;
  }

  function notice(element, message, failed = false) {
    element.className = failed ? "notice bad" : "notice ok";
    element.textContent = message;
    element.hidden = false;
  }

  function objectMutation(form) {
    const values = new FormData(form);
    const revision = Number(values.get("expected_revision"));
    const kind = form.dataset.apiKind;
    if (kind === "name" || kind === "status") {
      const key = kind === "name" ? "name" : "bstatus";
      return {apply: true, expected_revision: revision, [key]: values.get(key)};
    }
    if (kind === "json") {
      return {apply: true, expected_revision: revision,
        json_addl: JSON.parse(values.get("json_addl")), reason: values.get("reason")};
    }
    if (kind === "lineage") {
      const raw = String(values.get("v0_edge") ?? "").trim();
      return {related_euid: values.get("related_euid"),
        expected_revision: revision,
        related_expected_revision: Number(values.get("related_expected_revision")),
        direction: values.get("direction"), relationship_type: values.get("relationship_type"),
        ...(raw ? {v0_edge: JSON.parse(raw)} : {})};
    }
    throw new Error("Unsupported object operation.");
  }

  document.querySelectorAll("form[data-tapdb-api-form]").forEach(form => {
    const result = form.querySelector("[data-api-result]");
    form.addEventListener("submit", async event => {
      event.preventDefault();
      const buttons = [...form.querySelectorAll("button[type=submit]")];
      buttons.forEach(button => { button.disabled = true; });
      try {
        const response = await request(form.action, objectMutation(form));
        notice(result, response.lineage_euid ? `Created lineage ${response.lineage_euid}.`
          : `Saved ${response.euid}, revision ${response.record_revision}.`);
        window.location.assign(form.dataset.objectUrl);
      } catch (error) {
        notice(result, error.message, true);
      } finally {
        buttons.forEach(button => { button.disabled = false; });
      }
    });
  });
  window.TapdbApi = {request, notice};
})();

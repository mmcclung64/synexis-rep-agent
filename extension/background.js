// Open the side panel when the toolbar icon is clicked.
// Chrome MV3 side panel API: https://developer.chrome.com/docs/extensions/reference/api/sidePanel
chrome.sidePanel
  .setPanelBehavior({ openPanelOnActionClick: true })
  .catch((error) => console.error("sidePanel.setPanelBehavior failed:", error));

// Open a URL in a new Chrome tab. Called from the sidebar via chrome.runtime.sendMessage
// so that PDFs and other file links open in the browser's built-in viewer instead of
// triggering a download (which happens when window.open() is called from within the
// extension's sandboxed side-panel context).
chrome.runtime.onMessage.addListener((message, _sender, _sendResponse) => {
  if (message && message.type === "open_tab" && message.url) {
    chrome.tabs.create({ url: message.url });
  }
});

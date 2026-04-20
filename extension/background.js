// Open the side panel when the toolbar icon is clicked.
// Chrome MV3 side panel API: https://developer.chrome.com/docs/extensions/reference/api/sidePanel
chrome.sidePanel
  .setPanelBehavior({ openPanelOnActionClick: true })
  .catch((error) => console.error("sidePanel.setPanelBehavior failed:", error));

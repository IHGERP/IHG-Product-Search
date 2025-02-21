// Copyright (c) 2025, Aerele and contributors
// For license information, please see license.txt

frappe.ui.form.on("Typesense Settings", {
  refresh(frm) {
    if (!frm.doc.is_sync) {
      frm.add_custom_button(__("Sync Items"), () => {
        frappe.call({
          method:
            "igh_search.igh_search.doctype.typesense_settings.typesense_settings.initialize_syncing_items",
          frezz: true,
          callback: function () {
            frm.reload_doc();
          },
        });
      });
    }
  },
});

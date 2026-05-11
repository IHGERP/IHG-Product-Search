import frappe

def get_settings():
    logs = frappe.get_all("Typesense Sync Log", filters={"trigger_type": "full_sync"}, order_by="creation desc", limit=5)
    for log in logs:
        doc = frappe.get_doc("Typesense Sync Log", log.name)
        print(f"Log: {doc.name}, Status: {doc.status}, Created: {doc.creation}")

if __name__ == "__main__":
    get_settings()

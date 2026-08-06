from dataclasses import dataclass


@dataclass(frozen=True)
class OrgsEndpointBuilder:
    def list_orgs(self) -> str:
        return "/orgs"

    def get_org(self, org_id: str) -> str:
        return f"/orgs/{org_id}"

    def update_org(self, org_id: str) -> str:
        return f"/orgs/{org_id}"

    def delete_org(self, org_id: str, name: str) -> str:
        return f"/orgs/{org_id}/{name}"

    def list_org_access(self, org_id: str) -> str:
        return f"/orgs/{org_id}/access"

    def update_org_access(self, org_id: str) -> str:
        return f"/orgs/{org_id}/access"

    def get_org_usage(self, org_id: str) -> str:
        return f"/orgs/{org_id}/usage"


@dataclass(frozen=True)
class WorkspacesEndpointBuilder:
    def get_workspaces_list(self, org_id: str) -> str:
        return f"/orgs/{org_id}/workspaces"

    def create_workspace(self, org_id: str) -> str:
        return f"/orgs/{org_id}/workspaces"

    def get_workspace(self, workspace_id: str) -> str:
        return f"/workspaces/{workspace_id}"

    def update_workspace(self, workspace_id: str) -> str:
        return f"/workspaces/{workspace_id}"

    def delete_workspace(self, workspace_id: str) -> str:
        return f"/workspaces/{workspace_id}"

    def remove_workspace(self, workspace_id: str) -> str:
        return f"/workspaces/{workspace_id}/remove"

    def restore_workspace(self, workspace_id: str) -> str:
        return f"/workspaces/{workspace_id}/restore"

    def list_workspace_access(self, workspace_id: str) -> str:
        return f"/workspaces/{workspace_id}/access"

    def update_workspace_access(self, workspace_id: str) -> str:
        return f"/workspaces/{workspace_id}/access"


@dataclass(frozen=True)
class DocsEndpointBuilder:
    def create_doc(self) -> str:
        return "/docs"

    def get_doc(self, doc_id: str) -> str:
        return f"/docs/{doc_id}"

    def update_doc_metadata(self, doc_id: str) -> str:
        return f"/docs/{doc_id}"

    def delete_doc(self, doc_id: str) -> str:
        return f"/docs/{doc_id}"

    def remove_doc(self, doc_id: str) -> str:
        return f"/docs/{doc_id}/remove"

    def restore_doc(self, doc_id: str) -> str:
        return f"/docs/{doc_id}/unremove"

    def move_doc(self, doc_id: str) -> str:
        return f"/docs/{doc_id}/move"

    def pin_doc(self, doc_id: str) -> str:
        return f"/docs/{doc_id}/pin"

    def unpin_doc(self, doc_id: str) -> str:
        return f"/docs/{doc_id}/unpin"

    def disable_doc(self, doc_id: str) -> str:
        return f"/docs/{doc_id}/disable"

    def enable_doc(self, doc_id: str) -> str:
        return f"/docs/{doc_id}/enable"

    def list_doc_access(self, doc_id: str) -> str:
        return f"/docs/{doc_id}/access"

    def update_doc_access(self, doc_id: str) -> str:
        return f"/docs/{doc_id}/access"

    def list_doc_users_for_view_as(self, doc_id: str) -> str:
        return f"/docs/{doc_id}/usersForViewAs"

    def download_doc(self, doc_id: str) -> str:
        return f"/docs/{doc_id}/download"

    def download_doc_as(self, doc_id: str, format: str) -> str:
        return f"/docs/{doc_id}/download/{format}"

    def download_doc_table_schema(self, doc_id: str) -> str:
        return f"/docs/{doc_id}/download/table-schema"


@dataclass(frozen=True)
class RecordsEndpointBuilder:
    def list_records(self, doc_id: str, table_id: str) -> str:
        return f"/docs/{doc_id}/tables/{table_id}/records"

    def add_records(self, doc_id: str, table_id: str) -> str:
        return f"/docs/{doc_id}/tables/{table_id}/records"

    def update_records(self, doc_id: str, table_id: str) -> str:
        return f"/docs/{doc_id}/tables/{table_id}/records"

    def add_update_records(self, doc_id: str, table_id: str) -> str:
        return f"/docs/{doc_id}/tables/{table_id}/records"

    def delete_records(self, doc_id: str, table_id: str) -> str:
        return f"/docs/{doc_id}/tables/{table_id}/records/delete"


@dataclass(frozen=True)
class TablesEndpointBuilder:
    def list_tables(self, doc_id: str) -> str:
        return f"/docs/{doc_id}/tables"

    def create_table(self, doc_id: str) -> str:
        return f"/docs/{doc_id}/tables"

    def update_table(self, doc_id: str) -> str:
        return f"/docs/{doc_id}/tables"


@dataclass(frozen=True)
class WebhooksEndpointBuilder:
    def list_webhooks(self, doc_id: str) -> str:
        return f"/docs/{doc_id}/webhooks"

    def create_webhook(self, doc_id: str) -> str:
        return f"/docs/{doc_id}/webhooks"

    def update_webhook(self, doc_id: str, webhook_id: str) -> str:
        return f"/docs/{doc_id}/webhooks/{webhook_id}"

    def delete_webhook(self, doc_id: str, webhook_id: str) -> str:
        return f"/docs/{doc_id}/webhooks/{webhook_id}"

    def clear_webhook_doc_queue(self, doc_id: str, webhook_id: str) -> str:
        return f"/docs/{doc_id}/webhooks/queue"

    def clear_webhook_queue(self, doc_id: str, webhook_id: str) -> str:
        return f"/docs/{doc_id}/webhooks/queue/{webhook_id}"


@dataclass(frozen=True)
class SQLEndpointBuilder:
    def execute_sql(self, doc_id: str) -> str:
        return f"/docs/{doc_id}/sql"

    def execute_sql_with_query_params(self, doc_id: str) -> str:
        return f"/docs/{doc_id}/sql"

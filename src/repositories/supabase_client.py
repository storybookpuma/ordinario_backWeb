import requests


class SupabaseRestClient:
    def __init__(self, url, service_role_key):
        self.base_url = f"{url.rstrip('/')}/rest/v1"
        self.headers = {
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json",
        }

    def select_one(self, table, **filters):
        rows = self.select(table, limit=1, **filters)
        return rows[0] if rows else None

    def select(self, table, limit=None, order=None, offset=None, columns=None, **filters):
        params = self._eq_filters(filters)
        if limit is not None:
            params["limit"] = str(limit)
        if offset is not None:
            params["offset"] = str(offset)
        if order is not None:
            params["order"] = order
        if columns is not None:
            params["select"] = columns

        response = requests.get(
            f"{self.base_url}/{table}",
            headers=self.headers,
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def insert_one(self, table, payload):
        response = requests.post(
            f"{self.base_url}/{table}",
            headers={**self.headers, "Prefer": "return=representation"},
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        rows = response.json()
        return rows[0] if rows else None

    def update(self, table, filters, payload):
        params = self._eq_filters(filters)
        response = requests.patch(
            f"{self.base_url}/{table}",
            headers={**self.headers, "Prefer": "return=representation"},
            params=params,
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def delete(self, table, **filters):
        response = requests.delete(
            f"{self.base_url}/{table}",
            headers=self.headers,
            params=self._eq_filters(filters),
            timeout=30,
        )
        response.raise_for_status()
        return response.text

    def rpc(self, function_name, payload):
        response = requests.post(
            f"{self.base_url}/rpc/{function_name}",
            headers=self.headers,
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def _eq_filters(self, filters):
        result = {}
        for key, value in filters.items():
            if key.endswith("_ilike"):
                result[key.replace("_ilike", "")] = f"ilike.{value}"
            elif key.endswith("_in"):
                result[key.replace("_in", "")] = f"in.({value})"
            else:
                result[key] = f"eq.{value}"
        return result


def create_supabase_client(app):
    url = app.config.get("SUPABASE_URL")
    service_role_key = app.config.get("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not service_role_key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required when using Supabase."
        )

    return SupabaseRestClient(url, service_role_key)

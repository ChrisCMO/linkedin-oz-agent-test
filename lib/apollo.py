"""Apollo.io API client with usage logging to Supabase."""

import os
import time
import random

import requests
from dotenv import load_dotenv

from db.connect import get_supabase

load_dotenv()


class ApolloClient:
    def __init__(self):
        self.base_url = os.environ.get("APOLLO_BASE_URL", "https://api.apollo.io")
        self.api_key = os.environ["APOLLO_API_KEY"]
        self.headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "X-Api-Key": self.api_key,
        }
        self.sb = get_supabase()

    # -- Internal helpers --

    def _log(self, request_type, credits_used=0, campaign_id=None,
             tenant_id=None, result_count=None, request_params=None,
             response_summary=None):
        row = {
            "request_type": request_type,
            "credits_used": credits_used,
        }
        if tenant_id:
            row["tenant_id"] = tenant_id
        if campaign_id:
            row["campaign_id"] = campaign_id
        if result_count is not None:
            row["result_count"] = result_count
        if request_params:
            row["request_params"] = request_params
        if response_summary:
            row["response_summary"] = response_summary
        try:
            self.sb.table("apollo_usage").insert(row).execute()
        except Exception as e:
            print(f"[ApolloClient] Failed to log usage: {e}")

    def _request(self, method, path, json_body=None, params=None, timeout=30):
        url = f"{self.base_url}{path}"
        resp = requests.request(
            method, url, headers=self.headers,
            json=json_body, params=params, timeout=timeout,
        )
        if resp.status_code >= 400:
            return {"error": f"HTTP {resp.status_code}", "detail": resp.text[:500]}
        return resp.json() if resp.content else {}

    # -- Search (FREE — 0 credits) --

    def search_people(self, person_titles=None, person_seniorities=None,
                      organization_num_employees_ranges=None,
                      person_locations=None, q_organization_keyword_tags=None,
                      page=1, per_page=25, tenant_id=None, campaign_id=None):
        """Search Apollo for people matching ICP criteria. Costs 0 credits."""
        body = {"page": page, "per_page": per_page}
        if person_titles:
            body["person_titles"] = person_titles
        if person_seniorities:
            body["person_seniorities"] = person_seniorities
        if organization_num_employees_ranges:
            body["organization_num_employees_ranges"] = organization_num_employees_ranges
        if person_locations:
            body["person_locations"] = person_locations
        if q_organization_keyword_tags:
            body["q_organization_keyword_tags"] = q_organization_keyword_tags

        result = self._request("POST", "/api/v1/mixed_people/api_search", json_body=body)

        if "error" not in result:
            self._log(
                request_type="people_search",
                credits_used=0,
                tenant_id=tenant_id,
                campaign_id=campaign_id,
                result_count=result.get("pagination", {}).get("total_entries", 0),
                request_params=body,
                response_summary={
                    "total_entries": result.get("pagination", {}).get("total_entries", 0),
                    "page": page,
                    "per_page": per_page,
                },
            )

        return result

    # -- Enrich (1 credit each) --

    def enrich_person(self, apollo_id, tenant_id=None, campaign_id=None):
        """Enrich a person by Apollo ID. Costs 1 credit."""
        body = {"id": apollo_id, "reveal_personal_emails": True}
        result = self._request("POST", "/api/v1/people/match", json_body=body)

        if "error" not in result:
            self._log(
                request_type="person_enrich",
                credits_used=1,
                tenant_id=tenant_id,
                campaign_id=campaign_id,
                result_count=1 if result.get("person") else 0,
                request_params={"apollo_id": apollo_id},
            )

        return result

    def enrich_batch(self, apollo_ids, tenant_id=None, campaign_id=None):
        """Enrich up to 20 people with delays between calls."""
        results = []
        for apollo_id in apollo_ids[:20]:
            result = self.enrich_person(apollo_id, tenant_id=tenant_id, campaign_id=campaign_id)
            person = result.get("person")
            if person:
                results.append(self._extract_person(person))
            time.sleep(random.uniform(0.5, 1.5))
        return results

    # -- Usage stats (FREE) --

    def get_usage_stats(self):
        """Get Apollo API usage/credit stats. Costs 0 credits."""
        return self._request("POST", "/api/v1/usage_stats/api_usage_stats", json_body={})

    # -- Health --

    def get_health(self):
        """Check Apollo API health."""
        return self._request("GET", "/api/v1/auth/health")

    # -- Helpers --

    @staticmethod
    def _extract_person(person):
        """Extract structured data from Apollo person response."""
        org = person.get("organization") or {}
        return {
            "apollo_id": person.get("id"),
            "name": person.get("name"),
            "first_name": person.get("first_name"),
            "last_name": person.get("last_name"),
            "title": person.get("title"),
            "seniority": person.get("seniority"),
            "linkedin_url": person.get("linkedin_url"),
            "email": person.get("email"),
            "city": person.get("city"),
            "state": person.get("state"),
            "photo_url": person.get("photo_url"),
            "headline": person.get("headline"),
            "company_name": org.get("name"),
            "company_industry": org.get("industry"),
            "company_employees": org.get("estimated_num_employees"),
            "company_employee_range": org.get("employee_count_range"),
            "company_website": org.get("website_url"),
            "company_linkedin": org.get("linkedin_url"),
            "company_domain": org.get("primary_domain"),
            "company_revenue": org.get("annual_revenue"),
            "company_founded": org.get("founded_year"),
            "company_keywords": org.get("keywords") or [],
            "apollo_org_id": org.get("id"),
            "raw_person": person,
        }

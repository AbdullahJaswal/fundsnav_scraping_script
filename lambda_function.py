import os
import re
import unicodedata
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg2
import requests
from bs4 import BeautifulSoup
from psycopg2.extras import execute_values


def slugify(name: str, existing_slugs: list[str]) -> str:
    try:
        name = (
            unicodedata.normalize("NFKD", name)
            .encode("ascii", "ignore")
            .decode("ascii")
        )

        name = re.sub("[^\w\s-]", "", name).strip().lower()

        slug = re.sub("[-\s]+", "-", name).split("-formerly")[0].strip()

        if slug in existing_slugs:
            count = 2

            while f"{slug}-{count}" in existing_slugs:
                count += 1

            slug = f"{slug}-{count}"

        existing_slugs.append(slug)

        return slug
    except Exception as e:
        print(f"slugify: {e}")
        return ""


def get_amcs(query):
    try:
        query.execute(
            "SELECT id, name, code, slug FROM mutual_funds_assetmanagementcompany ORDER BY name, id;"
        )
        result = query.fetchall()

        if result:
            result_seperated = list(zip(*result))

            return (
                list(result_seperated[0]),
                list(result_seperated[1]),
                list(result_seperated[2]),
                list(result_seperated[3]),
            )

        return [], [], [], []
    except Exception as e:
        print(f"get_amcs: {e}")
        return [], [], [], []


def get_amcs_codes(query):
    try:
        query.execute(
            "SELECT code FROM mutual_funds_assetmanagementcompany ORDER BY name, id;"
        )
        result = query.fetchall()

        if result:
            return list(result)

        return []
    except Exception as e:
        print(f"get_amcs_codes: {e}")
        return []


def get_funds(query, id_comparison=False):
    try:
        if id_comparison:
            query.execute("SELECT id, name FROM mutual_funds_fund ORDER BY name, id;")
            result = query.fetchall()

            if result:
                names_ids = dict()

                for row in result:
                    names_ids[row[1]] = row[0]

                return names_ids

            return dict()
        else:
            query.execute("SELECT code, slug FROM mutual_funds_fund ORDER BY name, id;")
            result = query.fetchall()

            if result:
                result_seperated = list(zip(*result))

                return list(result_seperated[0]), list(result_seperated[1])

            return [], []
    except Exception as e:
        print(f"get_funds: {e}")
        return []


def get_funds_names_ids(query):
    try:
        query.execute(
            "SELECT id, name, category_id FROM mutual_funds_fund ORDER BY name, id;"
        )
        result = query.fetchall()

        if result:
            names_ids = dict()

            for row in result:
                names_ids[f"{row[1]}~{row[2]}"] = row[0]

            return names_ids

        return dict()
    except Exception as e:
        print(f"get_funds_names_ids: {e}")
        return dict()


def get_categories(query):
    try:
        query.execute(
            "SELECT id, name, code, slug FROM mutual_funds_category ORDER BY name, id;"
        )
        result = query.fetchall()

        if result:
            result_seperated = list(zip(*result))

            return (
                list(result_seperated[0]),
                list(result_seperated[1]),
                list(result_seperated[2]),
                list(result_seperated[3]),
            )

        return [], [], [], []
    except Exception as e:
        print(f"get_categories: {e}")
        return [], [], [], []


def get_categories_names_ids(query):
    try:
        query.execute("SELECT id, name FROM mutual_funds_category ORDER BY name, id;")
        result = query.fetchall()

        if result:
            names_ids = dict()

            for row in result:
                names_ids[row[1]] = row[0]

            return names_ids

        return dict()
    except Exception as e:
        print(f"get_categories_names_ids: {e}")
        return dict()


def get_mc_codes(query):
    try:
        date_2_months_ago = datetime.now(timezone.utc) - timedelta(days=90)
        date_2_months_ago = date_2_months_ago.replace(day=1).isoformat()

        query.execute(
            """
            SELECT code
            FROM (SELECT DISTINCT ON (fund_id) fund_id, code, month
                  FROM mutual_funds_marketcap
                  WHERE month >= %s
                  ORDER BY fund_id, month DESC, code) AS t
            ORDER BY month DESC;
            """,
            (date_2_months_ago,),
        )
        result = query.fetchall()

        if result:
            out = [item for t in result for item in t]
            return out

        return []
    except Exception as e:
        print(f"get_mc_codes: {e}")
        return []


def get_all_mc_codes(query):
    try:
        query.execute("SELECT code FROM mutual_funds_marketcap ORDER BY id;")
        result = query.fetchall()

        if result:
            result = [int(item) for t in result for item in t]
            return result

        return []
    except Exception as e:
        print(f"get_all_mc_codes: {e}")
        return []


def add_mcs(conn=None):
    try:
        query = conn.cursor()

        URL = "https://www.mufap.com.pk/AUMs_report.php"
        HEADERS = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"
        }

        print("Updating Market Caps...")
        mc_codes = get_mc_codes(query)
        updated_at = datetime.now(timezone.utc).isoformat()

        for mc_code in mc_codes:
            params = {"Fund_Code": mc_code}

            response = requests.get(url=URL, params=params, headers=HEADERS)
            soup = BeautifulSoup(response.content, "html.parser")

            rows = soup.findAll("tr")

            rows[3].find("b").decompose()
            fund_name = " ".join(
                rows[3]
                .find("td")
                .text.strip()
                .encode("ascii", "ignore")
                .decode("ascii")
                .split()
            )
            print(f"Fund Name: {fund_name}")

            rows[5].find("b").decompose()
            month_year = datetime.strptime(
                rows[5].find("td").text.strip(), "%B, %Y"
            ).date()
            month_year_str = month_year.strftime("%Y-%m-%d")

            mc_values = []

            for col in rows[7:]:
                value = col.findAll("td")[1].text.replace(",", "").strip()

                if value[0] == "(":
                    value = value.replace("-", "")
                    value = f"-{value[1:-1]}"

                value = str(Decimal(value) * 1000)

                mc_values.append(value)

            print(f"Updating {fund_name}|{month_year_str} Market Cap")
            mc_query = """
            UPDATE mutual_funds_marketcap SET (
                month,
                cash,
                cash_currency,
                placements_with_banks_and_dfis,
                placements_with_banks_and_dfis_currency,
                placements_with_nbfs,
                placements_with_nbfs_currency,
                reverse_repos_against_government_securities,
                reverse_repos_against_government_securities_currency,
                reverse_repos_against_all_other_securities,
                reverse_repos_against_all_other_securities_currency,
                tfcs,
                tfcs_currency,
                government_backed_guaranteed_securities,
                government_backed_guaranteed_securities_currency,
                equities,
                equities_currency,
                pibs,
                pibs_currency,
                tbills,
                tbills_currency,
                commercial_papers,
                commercial_papers_currency,
                spread_transactions,
                spread_transactions_currency,
                cfs_margin_financing,
                cfs_margin_financing_currency,
                others_including_receivables,
                others_including_receivables_currency,
                liabilities,
                liabilities_currency,
                total,
                total_currency,
                updated_at
            ) = (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            ) WHERE code = %s;
            """

            query.execute(
                mc_query,
                (
                    month_year.isoformat(),
                    *[
                        val
                        for pair in zip(mc_values, ["PKR"] * len(mc_values))
                        for val in pair
                    ],
                    updated_at,
                    mc_code,
                ),
            )
    except Exception as e:
        print(f"add_mcs: {e}")
    finally:
        if conn is not None:
            conn.commit()
            print(
                datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                end="\n\n",
            )


def update_amc(query, code, name):
    try:
        query.execute(
            "UPDATE mutual_funds_assetmanagementcompany SET (name, updated_at) = (%s, %s) WHERE code = %s;",
            (name, datetime.now().isoformat(), code),
        )
    except Exception as e:
        print(f"update_amc: {e}")


def update_category(query, code, name):
    try:
        query.execute(
            "UPDATE mutual_funds_category SET (name, updated_at) = (%s, %s) WHERE code = %s;",
            (name, datetime.now().isoformat(), code),
        )
    except Exception as e:
        print(f"update_category: {e}")


def update_fund(query, id, name):
    try:
        query.execute(
            "UPDATE mutual_funds_fund SET (name, updated_at) = (%s, %s) WHERE id = %s;",
            (name, datetime.now().isoformat(), id),
        )
    except Exception as e:
        print(f"update_fund: {e}")


def add_amcs_cats_funds_mc_codes(conn=None):
    try:
        query = conn.cursor()

        URL = "https://www.mufap.com.pk/aum_report.php"
        HEADERS = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"
        }
        fund_types = [
            "Open End Schemes",
            "Voluntary Pension Funds",
            "Closed End Schemes",
            "Dedicated Equity Funds",
            "Exchange Traded Fund(ETF)",
        ]

        for tab in ("01", "02", "04", "05"):  # Fund Types Tab Indexes
            params = {"tab": tab}

            response = requests.get(url=URL, params=params, headers=HEADERS)

            if response.status_code == 200:
                amcs_ids, amcs_names, amcs_codes, amcs_slugs = get_amcs(query)
                (
                    categories_ids,
                    categories_names,
                    categories_codes,
                    categories_slugs,
                ) = get_categories(query)

                amcs = []
                categories = []
                funds = []
                amcs_mc_details = []
                created_at = datetime.now(timezone.utc)
                updated_at = created_at

                soup = BeautifulSoup(response.content, "html.parser")

                if tab == "01":
                    options = soup.find_all("option")

                    count = 0
                    for option in options:
                        if option.text.strip() == "":
                            count += 1
                        elif option.text.strip() == "Month":
                            break
                        elif count == 2:
                            pass
                        else:
                            code = option["value"].strip()
                            name = " ".join(
                                option.text.strip("_")
                                .strip()
                                .encode("ascii", "ignore")
                                .decode("ascii")
                                .split()
                            )

                            if count == 1 and code:
                                if code in amcs_codes and name not in amcs_names:
                                    print(f"Updating AMC {code}: {name}")
                                    update_amc(query, code, name)

                                if code not in amcs_codes:
                                    slug = slugify(name=name, existing_slugs=amcs_slugs)

                                    if slug:
                                        amcs.append(
                                            (code, name, slug, created_at, updated_at)
                                        )
                            elif count == 3 and code:
                                if (
                                    code in categories_codes
                                    and name not in categories_names
                                ):
                                    print(f"Updating Category {code}: {name}")
                                    update_category(query, code, name)

                                if code not in categories_codes:
                                    slug = slugify(
                                        name=name, existing_slugs=categories_slugs
                                    )
                                    cat_type = (
                                        "Islamic"
                                        if "shariah" in name.lower()
                                        else "Conventional",
                                    )

                                    if slug:
                                        categories.append(
                                            (
                                                code,
                                                name,
                                                slug,
                                                cat_type,
                                                created_at,
                                                updated_at,
                                            )
                                        )

                    print(f"Inserting {len(amcs)} AMCs")
                    if amcs:
                        execute_values(
                            query,
                            "INSERT INTO mutual_funds_assetmanagementcompany (code, name, slug, created_at, updated_at) VALUES %s",
                            amcs,
                        )
                    print(f"Inserting {len(categories)} Categories")
                    if categories:
                        execute_values(
                            query,
                            "INSERT INTO mutual_funds_category (code, name, slug, type, created_at, updated_at) VALUES %s",
                            categories,
                        )
                    conn.commit()

                amcs_ids, amcs_names, amcs_codes, amcs_slugs = get_amcs(query)
                (
                    categories_ids,
                    categories_names,
                    categories_codes,
                    categories_slugs,
                ) = get_categories(query)
                existing_funds, existing_funds_slugs = get_funds(query)
                fund_names_ids_with_cats = get_funds_names_ids(query)
                existing_mcs = get_all_mc_codes(query)
                table_rows = soup.find("table", {"class": "mydata"}).find_all("tr")
                month_data = table_rows[0].find_all("td")[-1].text.split("(")[0].strip()
                month_date = datetime.strptime(month_data, "%B %Y").isoformat()

                amc_id = None

                for row in table_rows[1:]:  # Skip Header Row
                    if row.has_attr("id"):
                        fund_code = row.get("id").strip()
                        cols = row.find_all("td")

                        fund_name = " ".join(
                            cols[0]
                            .text.strip("_")
                            .strip()
                            .encode("ascii", "ignore")
                            .decode("ascii")
                            .split()
                        )

                        if tab == "02":
                            cat_index = 2
                        else:
                            cat_index = 1

                        category_name = " ".join(
                            cols[cat_index]
                            .text.strip("-")
                            .strip()
                            .encode("ascii", "ignore")
                            .decode("ascii")
                            .split()
                        )
                        category_id = categories_ids[
                            categories_names.index(category_name)
                        ]

                        fund_id = fund_names_ids_with_cats.get(
                            f"{fund_name}~{category_id}", None
                        )

                        if fund_id and amc_id:
                            fund_name_fixed = fund_name.replace(
                                "FundClass", "Fund Class"
                            )
                            print(f"{fund_name} -> {fund_name_fixed}")

                            if fund_name_fixed != fund_name:
                                print(f"Updating Fund {fund_id}: {fund_name_fixed}")
                                update_fund(query, fund_id, fund_name_fixed)
                                fund_name = fund_name_fixed

                            href_text = cols[-1].find("a")

                            if href_text and href_text.get("href"):
                                amc_mc_detail_id = int(
                                    "".join(filter(str.isdigit, href_text.get("href")))
                                )

                                if amc_mc_detail_id not in existing_mcs:
                                    amcs_mc_details.append(
                                        (
                                            amc_mc_detail_id,
                                            month_date,
                                            fund_id,
                                            created_at,
                                            updated_at,
                                        )
                                    )

                        if fund_code and fund_code not in existing_funds:
                            slug = slugify(
                                name=fund_name, existing_slugs=existing_funds_slugs
                            )

                            inception_date = cols[cat_index + 1].text.strip()
                            if inception_date:
                                inception_date = (
                                    datetime.strptime(inception_date, "%B %d, %Y")
                                    .date()
                                    .isoformat()
                                )

                            if amc_id and slug:
                                funds.append(
                                    (
                                        fund_code,
                                        fund_name,
                                        slug,
                                        inception_date if inception_date else None,
                                        category_id,
                                        int(tab),
                                        amc_id,
                                        created_at,
                                        updated_at,
                                    )
                                )
                    else:
                        if len(row.find_all("td")) > 1:
                            pass
                        else:
                            amc_name = " ".join(
                                row.find("td")
                                .text.strip("_")
                                .strip()
                                .encode("ascii", "ignore")
                                .decode("ascii")
                                .split()
                            )
                            amc_name = amc_name.strip("_").strip()

                            try:
                                amc_id = amcs_ids[amcs_names.index(amc_name)]
                            except Exception as e:
                                print(f"add_amcs_cats_funds_mc_codes: {e}")

                print(f"Inserting {len(funds)} {fund_types[int(tab) - 1]} Funds")
                if funds:
                    execute_values(
                        query,
                        "INSERT INTO mutual_funds_fund (code, name, slug, inception_date, category_id, fund_type_id, amc_id, created_at, updated_at) VALUES %s",
                        funds,
                    )
                print(f"Inserting {len(amcs_mc_details)} AMC Market Cap Details IDs")
                if amcs_mc_details:
                    execute_values(
                        query,
                        "INSERT INTO mutual_funds_marketcap (code, month, fund_id, created_at, updated_at) VALUES %s",
                        amcs_mc_details,
                    )
    except Exception as e:
        print(f"add_amcs_cats_funds_mc_codes: {e}")
    finally:
        if conn is not None:
            conn.commit()
            print(
                datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                end="\n\n",
            )


def lambda_handler(event=None, context=None):
    print("Start DateTime: " + datetime.utcnow().strftime("%c %Z"), end="\n\n")

    try:
        # Scrap All Funds
        conn = None

        try:
            conn = psycopg2.connect(
                database=os.environ.get("DB_NAME"),
                user=os.environ.get("DB_USER"),
                password=os.environ.get("DB_PASS"),
                host=os.environ.get("DB_HOST"),
                port=os.environ.get("DB_PORT"),
            )

            add_amcs_cats_funds_mc_codes(conn)
            add_mcs(conn)

            print(
                "Whole Process Completed: " + datetime.now().strftime("%c %Z"),
                end="\n\n",
            )
        except Exception as e:
            print(e)
            print("Process ERROR: " + datetime.now().strftime("%c %Z"), end="\n\n")
        finally:
            if conn is not None:
                conn.close()

        return True
    except Exception as e:
        print(e)
        print("Startup ERROR: " + datetime.utcnow().strftime("%c %Z"), end="\n\n")

    return False

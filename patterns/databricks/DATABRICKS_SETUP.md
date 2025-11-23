# Databricks Setup Guide

This guide walks you through getting the credentials needed to connect dlt pipelines to Databricks.

## Prerequisites

- Active Databricks workspace (Azure, AWS, or GCP)
- User account with permissions to:
  - Create and use SQL Warehouses
  - Generate personal access tokens
  - Create schemas/tables in Unity Catalog

## Step-by-Step Credential Setup

### 1. Find Your Workspace Hostname

The **server_hostname** is your Databricks workspace URL without `https://`.

**Steps:**
1. Log in to your Databricks workspace
2. Look at your browser URL bar
3. Copy the hostname part

**Examples:**
- Azure: `adb-1234567890123456.7.azuredatabricks.net`
- AWS: `dbc-abc12345-def6.cloud.databricks.com`
- GCP: `123456789.gcp.databricks.com`

**What to copy to secrets.toml:**
```toml
server_hostname = "adb-1234567890123456.7.azuredatabricks.net"
```

---

### 2. Get SQL Warehouse HTTP Path

The **http_path** identifies your SQL Warehouse compute resource.

**Steps:**
1. In Databricks workspace, click **SQL Warehouses** in left sidebar
2. If you don't have a warehouse:
   - Click **Create SQL Warehouse**
   - Give it a name (e.g., "dlt-pipeline-warehouse")
   - Choose size: **Small** (2X-Small for testing)
   - Leave other defaults
   - Click **Create**
3. Click on your SQL Warehouse name
4. Go to **Connection Details** tab
5. Find **HTTP Path** (looks like `/sql/1.0/warehouses/abc123def456`)
6. Click **Copy** button

**What to copy to secrets.toml:**
```toml
http_path = "/sql/1.0/warehouses/abc123def456"
```

**Important Notes:**
- SQL Warehouse must be **running** (Auto-stop is fine)
- First run may be slow as warehouse starts up
- Choose size based on data volume:
  - **2X-Small/Small**: Development, small datasets
  - **Medium**: Production, larger datasets
  - Enable **Auto-stop** to save costs

---

### 3. Create Personal Access Token

The **access_token** authenticates your pipeline to Databricks.

**Steps:**
1. Click your **username** in top-right corner
2. Select **Settings**
3. Go to **Developer** section in left menu
4. Click **Access Tokens** (or **Manage** if already on the page)
5. Click **Generate New Token**
6. Fill in:
   - **Comment**: "dlt N-PORT Pipeline" (or descriptive name)
   - **Lifetime**: 90 days (or as per your security policy)
7. Click **Generate**
8. **IMPORTANT**: Copy the token immediately (starts with `dapi...`)
   - You won't be able to see it again!
   - Store it securely

**What to copy to secrets.toml:**
```toml
access_token = "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"  # Replace with your actual token
```

**Security Best Practices:**
- Use short-lived tokens (30-90 days)
- Rotate tokens regularly
- Never commit tokens to git
- Use service principals for production (advanced)

---

### 4. Choose Catalog and Schema

Databricks uses a 3-level namespace: `catalog.schema.table`

**Catalog** (Unity Catalog):
- Default catalog: `main`
- Or create custom catalog: `nport`, `data_lake`, etc.

**Schema** (configured in config.toml):
- Default: `nport_bronze`
- The pipeline will create this if it doesn't exist

**Steps to create custom catalog (optional):**
1. In Databricks, go to **Data** in left sidebar
2. Click **Create** > **Catalog**
3. Enter name: `nport`
4. Click **Create**

**What to use:**
```toml
# In secrets.toml (optional, defaults to "main")
catalog = "main"

# In config.toml (already set)
[destination.databricks]
dataset = "nport_bronze"  # This is the schema name
```

**Final location:**
- Tables will be in: `main.nport_bronze.submission`, `main.nport_bronze.identifiers`, etc.
- Or if custom catalog: `nport.nport_bronze.submission`, etc.

---

## Complete secrets.toml Example

After gathering all values, your `.dlt/secrets.toml` should look like:

```toml
[destination.databricks.credentials]
server_hostname = "adb-1234567890123456.7.azuredatabricks.net"  # Your workspace URL
http_path = "/sql/1.0/warehouses/abc123def456"  # Your SQL Warehouse path
access_token = "dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"  # Your personal access token
catalog = "main"  # Or your catalog name
```

## Testing Your Connection

Before running the pipeline, test your credentials:

```bash
# Make sure Databricks is the active destination
python config_helper.py --set-destination databricks

# Test connection
python test_databricks_connection.py
```

**Expected output:**
```
[+] Connection successful!
[*] Available catalogs:
    - main
    - hive_metastore
[*] Using catalog: main
[*] Schemas in 'main' catalog:
    - default
    - information_schema
[*] CONNECTION TEST PASSED
```

## Troubleshooting

### Error: "Unable to connect"

**Check:**
1. Is your SQL Warehouse running?
   - Go to SQL Warehouses, check status
   - Click "Start" if stopped
2. Is your workspace URL correct?
   - Should NOT include `https://`
   - Should NOT have trailing slash
3. Is your token still valid?
   - Check expiration date in Settings > Developer > Access Tokens
   - Generate new token if expired

### Error: "Invalid HTTP path"

**Check:**
1. HTTP path starts with `/sql/1.0/warehouses/`
2. Copied from the correct SQL Warehouse
3. No extra spaces or characters

### Error: "Catalog not found"

**Solution:**
1. Use `catalog = "main"` (default catalog always exists)
2. Or create the catalog first in Databricks UI
3. Ensure you have permissions to access the catalog

### Error: "Permission denied"

**Check:**
1. Your user has **Can Use** permission on SQL Warehouse
2. Your user has **USE CATALOG** permission
3. Your user has **CREATE SCHEMA** permission (for first run)

**To grant permissions:**
1. Contact your Databricks admin, OR
2. If you're admin:
   - SQL Warehouse: Permissions tab > Add user
   - Catalog: Data Explorer > Catalog > Permissions

### Error: "Network/SSL issues"

**Check:**
1. Corporate firewall blocking Databricks?
2. VPN required to access workspace?
3. Check with IT/network admin

## Cost Considerations

### SQL Warehouse Pricing

**Databricks charges for:**
- **DBU** (Databricks Units) per second while warehouse is running
- **Cloud compute** costs (AWS/Azure/GCP VM costs)

**Cost-saving tips:**
1. **Enable Auto-stop**: Warehouse stops after 10-20 minutes of inactivity
2. **Start small**: Use 2X-Small/Small for development
3. **Serverless**: Consider Serverless SQL (if available in your region)
4. **Schedule**: Run pipelines during off-peak hours if possible

**Example costs (approximate, varies by region/cloud):**
- 2X-Small warehouse: ~$0.50-1.00/hour
- Small warehouse: ~$1.00-2.00/hour
- With auto-stop, actual cost depends on active usage time

### Free Trial

Most Databricks accounts come with:
- 14-day free trial, OR
- $400-500 in free credits
- Enough for extensive testing!

## Next Steps

Once connection test passes:

1. **Run your first pipeline:**
   ```bash
   python nport_pipeline.py
   ```

2. **View data in Databricks:**
   - Go to **Data** > **main** > **nport_bronze**
   - Click on table name (e.g., `submission`)
   - Click **Sample data** to preview

3. **Query with SQL:**
   - Go to **SQL Editor**
   - Run: `SELECT * FROM main.nport_bronze.submission LIMIT 10;`

4. **Monitor loads:**
   - Check `_dlt_loads` table for pipeline run history
   - View `_dlt_version` for schema versions

## Additional Resources

- [Databricks SQL Warehouses Documentation](https://docs.databricks.com/sql/admin/sql-endpoints.html)
- [Unity Catalog Guide](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [dlt Databricks Destination](https://dlthub.com/docs/dlt-ecosystem/destinations/databricks)
- [Personal Access Tokens](https://docs.databricks.com/dev-tools/auth/pat.html)

---

**Need Help?**
- Check dlt docs: https://dlthub.com/docs/
- Databricks Community: https://community.databricks.com/
- dlt Slack: https://dlthub.com/community

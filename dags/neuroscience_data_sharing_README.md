# Neuroscience Data Sharing Analysis

Script to fetch neuroscience articles from PMC and bioRxiv, download full-text XML, and analyze for data sharing indicators.

## Setup

### 1. Install Dependencies

```bash
pip install requests boto3 python-dotenv tqdm backoff
```

### 2. Configure API Keys

Create a `.env` file in the project root with the following keys:

```bash
# NCBI API Key (required for PMC, optional but recommended)
NCBI_API_KEY=your_ncbi_api_key

# AWS Credentials (required for bioRxiv S3 access)
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
```

#### Getting an NCBI API Key

1. Go to https://www.ncbi.nlm.nih.gov/account/
2. Sign in or create an NCBI account
3. Go to Settings > API Key Management
4. Click "Create an API Key"
5. Copy the key to your `.env` file

Without an API key, NCBI limits requests to 3/second. With a key, you get 10/second.

#### Getting AWS Credentials

1. Log in to the AWS Console: https://console.aws.amazon.com/
2. Go to IAM > Users > Your User > Security Credentials
3. Click "Create access key"
4. Copy the Access Key ID and Secret Access Key to your `.env` file

Note: bioRxiv S3 is a requester-pays bucket. You will be charged ~$0.09/GB for data transfer. The script uses efficient range requests to minimize costs (~$0.01/month for indexing).

## Usage

### Basic Usage

```bash
# Fetch from both sources (default: 10 articles each)
python dags/neuroscience_data_sharing.py

# Fetch only from bioRxiv
python dags/neuroscience_data_sharing.py --source biorxiv

# Fetch only from PMC
python dags/neuroscience_data_sharing.py --source pmc
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `--source` | Source to fetch from: `biorxiv`, `pmc`, or `both` | `both` |
| `--start-date` | Start date (YYYY-MM-DD) | `2024-01-01` |
| `--end-date` | End date (YYYY-MM-DD) | today |
| `--max-articles` | Target number of articles with full text per source | `10` |
| `--output` | Save results to JSON file | none |
| `--refresh-cache` | Ignore cache and re-download all full-text | false |
| `--show-cache-stats` | Show cache statistics and exit | false |

### Examples

```bash
# Fetch 50 bioRxiv articles from December 2024
python dags/neuroscience_data_sharing.py \
    --source biorxiv \
    --start-date 2024-12-01 \
    --end-date 2024-12-31 \
    --max-articles 50

# Fetch articles and save to JSON
python dags/neuroscience_data_sharing.py \
    --source both \
    --max-articles 100 \
    --output results.json

# Check what's cached
python dags/neuroscience_data_sharing.py --show-cache-stats
```

## Output

The script outputs:
- Summary of articles found
- Data sharing statistics (articles with data availability sections, data sharing, data reuse)
- Sample articles with detected repositories and accession numbers

When using `--output`, results are saved as JSON with fields:
- `id`, `title`, `doi`, `source`, `pub_date`, `category`
- `has_data_availability_section`, `shares_data`, `reuses_data`
- `detected_repositories`, `detected_accessions`, `data_sharing_score`

## Caching

Full-text XML is cached locally to avoid re-downloading:
- bioRxiv: `data/biorxiv_xml/`
- PMC: `data/pmc_xml/`
- DOI indices: `data/biorxiv_doi_index_{month}.json` (expires after 7 days)

Use `--refresh-cache` to force re-download.

## Detected Repositories

The script detects mentions of:
- **Neuroscience**: DANDI, OpenNeuro, NeuroData, NeuroMorpho, Allen Brain
- **General**: Zenodo, Figshare, Dryad, OSF, Dataverse
- **Genomics**: ArrayExpress, ENA, GenBank, UniProt
- **Imaging**: EMPIAR, BioImage Archive, OMERO
- **Code**: GitHub, GitLab, Code Ocean

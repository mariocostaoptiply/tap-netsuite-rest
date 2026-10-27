# tap-netsuite-rest

`tap-netsuite-rest` is a Singer tap for NetSuite.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

- [ ] `Developer TODO:` Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

```bash
pipx install tap-netsuite-rest
```

## Configuration

### Accepted Config Options

- [ ] `Developer TODO:` Provide a list of config options accepted by the tap.

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-netsuite-rest --about
```

### Inventory Location Stock and Reorder Point

The `inventory_item_locations` stream exposes NetSuite's location-specific
`reorderpoint` as a nullable string. Rediscover the catalog and select this field
to include `inventoryitemlocations.reorderpoint` in the SuiteQL query. An omitted
value is not the same as `"0"`.

The replication key is `inventoryitemlocations.lastmodifieddate`, not
`lastquantityavailablechange`. Backorder-only changes with a newer modification
timestamp are extracted even when the availability timestamp is old or null.
The `item` stream and its bookmark are unchanged.

To migrate an existing job:

1. Rediscover the catalog and select `lastmodifieddate` alongside the required
   inventory quantities and any existing reorder-point selection.
2. Remove only the `inventory_item_locations` bookmark from Singer state.
3. Run the next sync. It scans every item range without applying `start_date`,
   emits rows whose replication key is null, and rebuilds the inventory bookmark
   from the greatest non-null `lastmodifieddate`. Subsequent runs are incremental.

Old inventory bookmarks and catalogs using `lastquantityavailablechange` are
rejected before extraction. A catalog projecting specific fields must include
`lastmodifieddate`; otherwise the tap requires rediscovery and selection.
The `force_sync_inventory` override preserves bookmarks, so it is not a
substitute for resetting the old inventory bookmark during this migration.

Range scans use deterministic `item, location` ordering so tied modification
timestamps cannot move records between offset pages.
They also filter `quantityavailable IS NOT NULL` before pagination. NetSuite can
otherwise report totals that include rows omitted from pages when this calculated
field is selected, causing fixed offsets to skip valid inventory rows.


### Source Authentication and Authorization

- [ ] `Developer TODO:` If your tap requires special access on the source system, or any special authentication requirements, provide those here.

## Usage

You can easily run `tap-netsuite-rest` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-netsuite-rest --version
tap-netsuite-rest --help
tap-netsuite-rest --config CONFIG --discover > ./catalog.json
```

### Assembly BOM components

The `assembly_bom_components` stream returns one row per assembly that has at
least one component in its active current BOM revision. It exposes only the
assembly ID as `id` and `parentitem`, which is the Product classification
contract consumed by the Mitari ETL. `DISTINCT` removes duplicate parents.

The stream intentionally uses `FULL_TABLE` replication. Neither
`assemblyitembom` nor `bomrevisioncomponentmember` exposes a change marker that
covers assignments, component edits, and deletions.

### Item volume

The `item` stream exposes Mitari's `custitem_volume_cm3` custom field as a
number. Rediscover and reselect the stream to add the field to an existing
catalog.

### Item Magento stock update

When present, `item.custitem_magento_stock_update` is always a nullable string
(`{"type": ["string", "null"]}`) without a `format`. This applies to both fresh
discovery and saved catalogs, including the emitted Singer schema. SuiteQL
selects the field directly rather than applying `TO_CHAR`, which NetSuite
rejects for this field. Existing catalog files do not need manual edits.

### Vendor transaction contact

Set `"vendor_transaction_contact": true` to add nullable string
`transaction_contact_id` to the `vendor` stream. This requires the account-specific
PO field `custbody_transaction_contact` and read access to transactions, contacts,
and `CompanyContactRelationship`. The option defaults to `false`.

For each supplier, the stream uses the contact on its most recently created
purchase order with a populated contact that is still active and linked to that
supplier. Creation-date ties use the highest PO ID. Suppliers with no usable
contact remain in the stream with `transaction_contact_id: null`; no contact is
guessed from the supplier's contact list.

When enabled, vendor replication is `FULL_TABLE`, including with an existing
incremental catalog and bookmark: changing a PO contact need not modify the
vendor's `lastmodifieddate`. Each run rereads vendors and ranks their PO history;
offset pages use `v.id, vsr.subsidiary` ordering. Other streams and the default
vendor incremental mode are unchanged.

The tap adds the field to both discovered and supplied schemas. Rediscover before
syncing if downstream tooling pins its own catalog. The import ETL must preserve
the field in the supplier snapshot, and the export ETL must read that snapshot
before mapping it to `custbody_transaction_contact`; this option changes extraction
only and does not create a purchase order or configure either ETL.

## Developer Resources

- [ ] `Developer TODO:` As a first step, scan the entire project for the text "`TODO:`" and complete any recommended steps, deleting the "TODO" references once completed.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_netsuite_rest/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-netsuite-rest` CLI interface directly using `poetry run`:

```bash
poetry run tap-netsuite-rest --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-netsuite-rest
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-netsuite-rest --version
# OR run a test `elt` pipeline:
meltano elt tap-netsuite-rest target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to 
develop your own taps and targets.

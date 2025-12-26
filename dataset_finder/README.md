# Dataset Finder

A tool to help quickly access and filter through datasets. Intended for ACS datasets (specifically, the tool was created for the [ACS bias correction release](https://github.com/AusClimateService/bias-correction-data-release)), but it should work with other datasets as long as they follow a consistent directory and file name format.

## Basic Usage

The paths.yml file has been set up with paths corresponding to the locations of the dynamically downscaled and bias adjusted data currently on ia39 and kj66. These have been labelled "ACS_DD" and "ACS_BC" respectively.

Running the line `get_datasets("ACS_DD")` in a JuypterLab cell will print a table containing all the datasets that could be found that follow the given directory structure. Each dataset will have information given about it, such as org, GCM, RCM or any other labels corresponding to {} enclosed names within the directory path format. 

Each of these can be filtered - for example, `get_datasets("ACS_DD", org = "BOM", gcm = "ACCESS")` will only show ACCESS models downscaled by the Bureau. Matches are not exact by default - the given example of `gcm = "ACCESS"` will match both "ACCESS-CM2" and "ACCESS-ESM1-5". This can be changed by adding "exact_match = True" into the argument list.

Multiple values can be supplied for each term. `get_datasets("ACS_DD", org = ("BOM", "CSIRO"), gcm = "ACCESS")` will show all ACCESS models for both the Bureau and CSIRO.

`get_datasets` returns a dataset_info_collection object which can be indexed to access individual dataset_info objects:

```
all_data = get_datasets("ACS_DD")
data = all_data[0]
data.print_info()
```

This will print information about the dataset - in this case, which years it contains.

The dataset_info can then by further filtered down to select years:
```
data.select(year = year_range(1980, 1990))
```
When selecting years, it is required to use year_range as the internal code will be confused by regular Python range (as that returns integers rather than strings which the code uses to match).

Finally, the dataset_info object can be loaded using xarray: `xr.open_mfdataset(data)`.

Further examples can be found in the provided example notebook, with a warning that it may be cumbersome to read directly on Git due to the large tables printed.

More examples are in the works.

## FAQ

### What's the easiest way to get started?
<details>
 <summary> Expand </summary>

The easiest way to get started using dataset_finder would be to clone this repository into the directory you're working in and simply add

```python
from dataset_finder.dataset_finder import *
```

to the beginning of your Python files. If you're putting your project files into the dataset_finder directory itself, just specify `dataset_finder` instead of `dataset_finder.dataset_finder`.

If running in an ARE JupyterLab notebook, you may need to direct your notebook to the correct location. The easiest way is to run 

```
cd your/work/directory
```

in a separate cell before the import statement.

</details>

### It's taking a long time to load...
<details>
 <summary> Expand </summary>

Unfortunately large data collections may take a while to loop through especially without any filtering. Solutions are being worked on to improve loading times (including to interface with intake catalogues where available).

</details>

### I'm getting extra variables in my selection sometimes (e.g. tasmax with tas, prsn with pr) - how do I stop this?
<details>
 <summary> Expand </summary>

By default, the various filtering and selecting options will match as long as the search term is a substring. This allows easily matching complicated names without needing to type them fully (such as "BARPA" instead of "BARPA-R"), or matching multiple similar sounding variants (such as "CCAM" for the various CSIRO and UQ-DEC runs), but may end up causing "tasmax" to match "tas" when "tas" itself was the desired variable. 

To disable substring matching, add `exact_match = True` as an argument and it will only match exactly.

</details>

### How can I loop through rows of the table?
<details>
 <summary> Expand </summary>

Entries (rows) in the dataset_info_collection can be accessed with by indexing, like a regular Python list. It can also be iterated over directly in a for loop:

```python
all_data = get_datasets(...)
for entry in all_data:
    ...
```

</details>

### How can I compare groups of data?
<details>
 <summary> Expand </summary>

There are two complementary methods to compare dataset_info_collection tables: `find_matches` and `find_missing`:
    
```python
matching_a = table_a.find_matches(table_b)
```
    
will return the entries in table_a that have at least one matching entry in table_b (defined as having equivalent properties (left of the line) for columns shared between the two tables). Conversely, `find_missing` will return the inverse (such that every entry of table_a will be in either `table_a.find_matches(table_b)` or `table_a.find_missing(table_b)` but not both.

Both methods support keyword arguments to specify which keys to match or not match against. The keyword argument `include_keys` can be used to specify columns to match against (instead of the default of picking common columns). The keyword argument `exclude_keys` can be used to specify columns *not* to match against from the default common columns list. Both keyword arguments cannot be specified simultaneously.

</details>

### Is there a way to get a list of the unique models in a certain collection?
<details>
 <summary> Expand </summary>

Yes - try something like `.get_all("gcm")`.

</details>

### How can I access information of a dataset from its info object?
<details>
 <summary> Expand </summary>

The `.data` attribute contains the unique information defining the dataset, such as gcm and org in common use cases. The `.get_info()` method can be called for a dictionary of lists for contents of the dataset, such as variables and years - `.print_info()` is a more readable version of this and makes it clear exactly what combinations of variables and years may exist within a dataset.

</details>

### How do I get the actual files for each dataset instead of loading into xarray?
<details>
 <summary> Expand </summary>

The files from the current selection of a dataset_info can be accessed using `.get_files()`. This is the same function and thus same list of files supplied to xarray when using `xr.open_mfdataset`. If used on a dataset_info_collection, `.get_files()` will return the files from every row concatenated into a single list.

</details>

### What does "unique" mean, defined in paths.yml?
<details>
 <summary> Expand </summary>

The "unique" field defines elements of a dataset that it does not make sense to load simultaneously, so the dataset_finder will have to either choose a value based on a defined priority, or simply raise an error to force the user to pick between them. The premier example is the "date_created" field (e.g. "v20241216" or "latest"), which may define different versions of the same file. 
    
If there are files with otherwise identical elements (such as `var = "pr"` and `year = 2015`), the dataset_finder will choose based on:
    
1) The user's choice if specified by `select`
2) The explicit order defined under the variable's "preferences" list if it exists
3) The implicit order defined by the variable's "default" parameter (where "high" means alphabetically highest) if it exists
    
If none of these are defined, an error will raised prompting the user to use `select` or `prioritise` to clear up the ambiguity.

</details>

### Does dataset_finder know the actual contents of the NetCDF files?
<details>
 <summary> Expand </summary>

No, the information is solely derived from reading the directory structure and file path. Discrepancies between this and the actual contents of the file will not be detected.

</details>

### Can I direct to paths that aren't in paths.yml?
<details>
 <summary> Expand </summary>

Yes, you can use the `filter_all` function instead of `get_datasets`, and supply the required arguments `format_dirs` and `format_file` (plus `unique` if needed), like in paths.yml.

</details>

### Can I use other file types than NetCDF?
<details>
 <summary> Expand </summary>

Yes! Since dataset_finder does not actually read the contents of any of the files, it can work seamlessly with any files that are well organised and named. For example, if saving images of plots for each model dataset_finder could in theory be used to quickly check that everything is present that should be present.

</details>

### Is Zarr supported?
<details>
 <summary> Expand </summary>

Yes - just ensure that there is a proceeding "/" after the file path to let the dataset_finder know to consider directories.

However, as multiple Zarr files cannot be opened simultaneously directly by xarray, care should be taken when actually trying to load datasets.

</details>

## What's next?

- Further expanded FAQ / usage guidance
- Memory / speed optimisations
- A tool to help come up with the path structures in the first place without needing to manually input
- Interface with intake catalogues
- Suggested output path structures for saving transformed data
- Potential improvements to the way date ranges are parsed
- More options for manipulating / viewing the large output tables - for example, there is currently no way to sort them

## Known Issues / Quirks

- Certain collections (such as the "GCM" path) have an incredibly large number of files and thus take a very long time to load without filters. I advise only using these if you have an idea of what you are looking for.
- Using "select" on a collection or dataset_info will modify it in-place instead of returning a modified clone, which may cause confusion if the same table is referenced with multiple queries.

Please contact Andrew Gammon (Andrew.Gammon@bom.gov.au) if you have any suggestions, issues or other feedback.

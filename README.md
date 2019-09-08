## Data Conversion Script

This script takes a custom spreadsheet used for collecting various meta data and converts it into a JSON file used in the [operaforever-ui](https://github.com/operatinder/operaforever-ui).

It can either be used to convert a local version of the spreadsheet or the Google spreadsheet. For using the Google API a key is required that is not part of this repo.

Usage:

* `python convert.py` reads the local "Zauberflöte Timestamps.xlsx" and writes a local "data.json".
* `python convert.py --source GoogleDoc ` reads the online spreadsheet.
* `python convert.py --target data-today.json ` writes the JSON file under a custom name.
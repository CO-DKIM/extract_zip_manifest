import pandas as pd
import zipfile
import os
import time
from datetime import datetime


class ExtractZip:
    """
    A class used to extract files from a zip according to manifest metadata

    Attributes
    ----------
    zip_file : str
        A zip file path which contains the items listed in the manifest
    manifest : str
        A comma-separated value file which contains at least the following
        information:
            * location of the item in the zip
            * path location for extraction
            * creation date of the item
            * modification date of the item
    input_col : str
        The name of the column in the manifest for item location in the zip.
        Default: "input"
    output_col : str
        The name of the column in the manifest for the path location where the
        item will be extracted to. Default: "output"
    creation_date_col : str
        The name of the column in the manifest for the item creation date.
        Default: "creation_date"
    modified_date_col : str
        The name of the column in the manifest for the item modified date.
        Default: "modified_date"

    Methods
    -------
    extract(date_format)
        Extracts the items listed in the manifest to their specified locations
        and sets the modified and created dates
    close()
        Close the zipfile and tidy up. This is called as part of the dunder
        methods `enter` and `exit` so it can be safely used as part of a `with`
        expression.
    """
    def __init__(
        self,
        zip_file,
        manifest,
        input_col="input",
        output_col="output",
        creation_date_col="creation_date",
        modified_date_col="modified_date"
    ):
        self.zip_file = zipfile.ZipFile(zip_file)
        self.df = pd.read_csv(manifest)
        self.input_col = input_col
        self.output_col = output_col
        self.creation_date_col = creation_date_col
        self.modified_date_col = modified_date_col
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def extract(self, date_format):
        """
        Extracts the items listed in the manifest to their specified locations
        and sets the modified and created dates.

        Parameters
        ----------
        date_format : str
            The format that the creation and modified dates are in using this
            cheatsheet for reference: https://strftime.org/
        """
        for zipped, out_path, creation_date, modified_date in self.df[
            [
                self.input_col,
                self.output_col,
                self.creation_date_col,
                self.modified_date_col
            ]
        ].itertuples(index=False, name=None):
            creation_date = time.mktime(
                datetime.strptime(
                    creation_date,
                    date_format
                ).timetuple()
            )
            modified_date = time.mktime(
                datetime.strptime(
                    modified_date,
                    date_format
                ).timetuple()
            )

            print(zipped, out_path, creation_date, modified_date)

            data = self.zip_file.read(zipped)

            if str(out_path).endswith('/'):
                os.makedirs(out_path, exist_ok=True)
            else:
                with open(out_path, 'wb') as f:
                    f.write(data)
                os.utime(out_path, (creation_date, modified_date))

    def close(self):
        if not getattr(self, "_closed", True):
            try:
                self.zip_file.close()
            finally:
                self._closed = True


if __name__ == "__main__":
    with ExtractZip("sample-1.zip", "example.csv") as e:
        e.extract("%Y-%m-%dT%H:%M:%S")

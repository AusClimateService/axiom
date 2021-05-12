"""Parser classes, for parsing metadata specifications."""
from bs4 import BeautifulSoup
import pandas as pd
import xml.etree.ElementTree as et
import lxml.etree as le


class Parser:

    """Abstract class for standardised subclasses."""

    def parse(self, filepath):
        """Parse the filepath into a dictionary of variables.

        Args:
            filepath (str) : Path to the file.

        Returns:
            dict : Dictionary of variables with metadata.

        Raises:
            NotImplementedError: [description]
        """
        raise NotImplementedError()


class CFSpecificationsParser(Parser):

    """Parser for the CFConventions standard name table."""

    def parse(self, filepath):
        """Parse the CF standard names table.

        Args:
            filepath (str): Path to the file.

        Returns:
            list : List of variables with
        """
        # xml = md.parse(filepath)
        xml = BeautifulSoup(open(filepath, 'r').read(), features='html.parser')

        parsed = dict()
        for entry in xml.standard_name_table.findAll('entry'):

            parsed[entry['id']] = dict(
                description=entry.description.text or None,
                units=entry.canonical_units.text or None,
                grib=entry.grib.text or None,
                amip=entry.amip.text or None,
            )

        return parsed


class CordexCSVParser(Parser):

    """Parse a Cordex CSV file with a header line into a list of variables."""

    def parse(self, filepath):
        """Parse a CSV file with header line into variable dict.

        Args:
            filepath (str): Path to the file.

        Returns:
            dict : Dictionary of variables and metadata.
        """
        df = pd.read_csv(filepath)

        parsed = dict()
        for record in df.to_dict('records'):
            parsed[record['variable']] = record

        return parsed


class XMLSchemaParser(Parser):

    """Parser for XSD files."""

    def parse(self, filepath):
        """Parse an xsd document.

        Args:
            filepath (str) : Path to the xsd file.

        Returns:
            lxml.etree.XMLSchema : Schema.
        """
        return le.XMLSchema(file=filepath)


class XMLParser(Parser):

    """Parser for XML files."""

    def parse(self, filepath, schema=None):
        """Parse an XML file.

        Args:
            filepath (str) : Path to the xml file.
            schema (lxml.etree.XMLSchema) : Optional schema.

        Returns:
            lxml.etree._Element : XML object
        """
        _parser = le.XMLParser(schema=schema)
        raw = open(filepath, 'rb').read()
        return le.fromstring(raw, parser=_parser)


class ParserFactory():

    """Convenience class to abstract the type of parser required."""

    def get_parser(self, name):
        """Get a parser suitable for the name.

        Args:
            name (str): Name of the parser.

        Returns:
            Parser: Subclass of Parser suitable for parsing the requested name.
        """

        instances = dict(
            xml=XMLParser,
            xsd=XMLSchemaParser,
            schema=XMLSchemaParser,
            cordex=CordexCSVParser,
            cf=CFSpecificationsParser,
        )

        return instances[name]()

"""Tests to ensure conversions are working."""
from axiom import xml2dict, dict2xml, str2xml, xml2str


def test_xml2dict():

    xml = '<root><mytag>test</mytag></root>'
    xml = str2xml(xml)

    result = xml2dict(xml)
    expected = dict(root=dict(mytag='test'))

    assert result == expected


def test_dict2xml():

    xml = '<root><mytag type="str">test</mytag></root>'
    expected = str2xml(xml)

    d = dict(mytag='test')
    result = dict2xml(d, root='root')

    assert xml2str(expected) == xml2str(result)

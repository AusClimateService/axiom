from axiom.drs.payload import generate_payloads

def test_generate_payloads():
    """Test generate_payloads."""

    payloads = generate_payloads('files.nc', 'output', 2000, 2021, 'CORDEX', 'ACCESS', 'AUS-10i', ['tas', 'tasmax'], 'CORDEX', ['1H', '6H'], 1, test_meta='test_meta_value')

    p1 = payloads[0]
    p2 = payloads[-1]

    assert len(payloads) == 44 # 22years x 2 time freq
    assert p1.start_year == 2000 and p1.project == 'CORDEX'
    assert p2.start_year == 2021 and p2.model == 'ACCESS'



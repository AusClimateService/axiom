from axiom.validate.validator import Validator
from axiom.validate.specification import V1
from axiom import load_data


if __name__ == '__main__':

    # Load the data
    ds = load_data('test.nc')

    specification = V1()
    validator = Validator(specification)

    validator.validate(ds)
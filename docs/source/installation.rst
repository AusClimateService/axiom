Installation
============

Axiom is installed via pip and should be installed inside a virtual environment such as conda.

As the project matures, these steps will become more automated and familiar.

Create environment
------------------

.. code-block:: shell
    
    # Create a conda environment
    conda create -n axiom_dev
    conda activate axiom_dev


Install Axiom
-------------

.. code-block:: shell

    # Clone the repository
    git clone git@github.com:AusClimateService/axiom.git

    # Navigate to the local copy
    cd axiom

    # Install
    pip install .

    # Move back up
    cd ..


Install Axiom Schemas
---------------------

Most of the utilities dependencies will be installed automatically, with the exception of the Axiom Schemas component, which must be installed separately.

.. code-block:: shell

    # Clone Axiom Schemas
    git clone git@github.com:AusClimateService/axiom-schemas.git

    cd axiom_schemas
    pip install -e .    
Installation
============

Axiom is installed via pip and should be installed inside a virtual environment such as conda.

Install Axiom from scratch.
------------------

.. code-block:: shell
    
    # Create a conda environment
    conda create -n axiom_dev
    conda activate axiom_dev

    # Install Axiom from pip
    pip install acs-axiom


Install Axiom on NCI
-------------

.. code-block:: shell

    # Select the hh5 modules
    module use /g/data/hh5/public/modules

    # Load the module
    module load conda/analysis3

    # Create a virtual environment and activate it
    conda create -n axiom_dev pip
    conda activate axiom_dev

    # Install Axiom
    pip install acs-axiom

    # Alternatively, install to user space
    pip install --user acs-axiom
sfbulk
=======
*Development Status :: 2 - Pre-Alpha*

sfbulk is a Python API wrapper for the `Salesforce.com's`_ Bulk API.
####################################################################


.. _Salesforce.com's: http://www.salesforce.com/


**Salesforce.com Inc.** is a global cloud computing company
best known for its customer relationship management (CRM) product.

Salesforce.com provides different types of `API`_.
The `Bulk API`_ provides programmatic access that allow you to quickly load a big parties of your organization's data into the Salesforce.

.. _API: https://www.salesforce.com/us/developer/docs/api_asynch/Content/asynch_api_intro.htm

.. _Bulk API: https://www.salesforce.com/us/developer/docs/api_asynch/

Why sfbulk?
-----------

**sfbulk** attempts to alleviate many of the problems programmers might experience
with Salesforce.com's' Bulk API by providing a 
cleaner, simpler and much more coherent API.

- You don't need to care of a creating **XML** for **SOAP** requests *(if you're here, you're aware of how it is with SOAP in Python...)*
- You can use the high `limits`_ of processing records (compared to Rest API)
- You can use it as an **programming** alternative to **clicking** `Data Loader`_
- You can control **sfbulk** operations by **Salesforce Bulk API monitor**


.. _limits: https://www.salesforce.com/us/developer/docs/api_asynch/Content/asynch_api_concepts_limits.htm
.. _Data Loader: https://developer.salesforce.com/page/Data_Loader
.. _Salesforce Bulk API monitor: 

**sfbulk** supports most all of the Salesforce.com's Bulk API functionality 
including:

- query
- insert
- update
- upsert
- delete

a large number of records asynchronously by submitting batches
which are processed in the background by Salesforce.

Example
-------

Here is a simple example of a basic **insert** operation for **Campaign** object processed with **sfbulk**.

.. code-block:: python


    from sfbulk import Bulk, logger
    from time import sleep

    # enable logging
    logger.setLevel(logging.DEBUG)


    # initialize bulk
    bulk = Bulk()
    bulk.login('username', 'password', 'security_token')

    # create job
    bulk.job_create('insert', 'Campaign')

    # create batch
    data = 'Name\none\ntwo\nthree'
    bulk.batch_create(data)

    # wait until job is completed
    while (not bulk.job_is_completed()):
        sleep(10)

    # close job
    bulk.job_close()


Documentation
-------------

Learn more from `documentation`_.

.. _documentation: http://clearcode.github.io/sfbulk/


License
-------

sfbulk is distributed under the `GPL License`_.

.. _GPL License: http://www.gnu.org/licenses/gpl.html

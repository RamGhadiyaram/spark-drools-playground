package com.examples

object SampleDataSet {
  val testdatasets =
    """
      |
      |
      |{
      |  "columns": [
      |    {
      |      "id": 9614404,
      |      "name": "col1",
      |      "created": 1542409377161,
      |      "modified": 1544031975490,
      |      "createdBy": "username",
      |      "modifiedBy": "username",
      |      "description": "Status code which shows active or closed",
      |      "businessColumnName": "id1",
      |      "columnOrder": 0,
      |      "desc": "columndescription",
      |      "type": "INT",
      |      "length": 2
      |    },
      |    {
      |      "id": 9614405,
      |      "name": "displaytext",
      |      "created": 1542409377162,
      |      "modified": 1544031975490,
      |      "createdBy": "username",
      |      "modifiedBy": "username",
      |      "description": "descriptions here",
      |      "businessColumnName": "displaytext",
      |      "columnOrder": 1,
      |      "descriptiveName": "displaytext",
      |      "type": "STRING",
      |      "length": 50
      |    },
      |    {
      |      "id": 9614406,
      |      "name": "status_description",
      |      "created": 1542409377163,
      |      "modified": 1544031975490,
      |      "createdBy": "username",
      |      "modifiedBy": "username",
      |      "description": "descriptions",
      |      "businessColumnName": "status_description",
      |      "columnOrder": 2,
      |      "type": "String",
      |      "length" : 50
      |    },
      |    {
      |      "id": 9614407,
      |      "name": "display_order",
      |      "created": 1542409377164,
      |      "modified": 1544031975490,
      |      "createdBy": "username",
      |      "modifiedBy": "username",
      |      "description": "This field specifies the display order sample 1,2,3,4",
      |      "businessColumnName": "display_order",
      |      "columnOrder": 3,
      |      "type": "INT",
      |      "length": 2,
      |      "descriptiveName": "descriptiveName"
      |    },
      |    {
      |      "id": 9614408,
      |      "name": "isActive",
      |      "created": 1542409377165,
      |      "modified": 1544031975490,
      |      "createdBy": "username",
      |      "modifiedBy": "username",
      |      "description": " is active or not.",
      |      "businessColumnName": "isActive",
      |      "columnOrder": 4,
      |      "type": "INT",
      |      "length": 1
      |
      |    },
      |    {
      |      "id": 9614409,
      |      "name": "toBeDisplayed",
      |      "created": 1542409377166,
      |      "modified": 1544031975490,
      |      "createdBy": "username",
      |      "modifiedBy": "username",
      |      "description": " display field to the user ",
      |      "businessColumnName": "toBeDisplayed",
      |      "columnOrder": 5,
      |      "descriptiveName": "toBeDisplayed",
      |      "type": "INT",
      |      "length": 1
      |    },
      |    {
      |      "id": 9614410,
      |      "name": "notes",
      |      "created": 1542409377166,
      |      "modified": 1544031975490,
      |      "createdBy": "username",
      |      "modifiedBy": "username",
      |      "description": "description HERE",
      |      "businessColumnName": "notes",
      |      "columnOrder": 6,
      |      "descriptiveName": "NOTES",
      |      "type": "STRING",
      |      "length": 300
      |
      |    },
      |    {
      |      "id": 9614411,
      |      "name": "created_date",
      |      "created": 1542409377167,
      |      "modified": 1544031975490,
      |      "createdBy": "username",
      |      "modifiedBy": "username",
      |      "description": "Date and time when the original record was created in the database.",
      |      "businessColumnName": "created_date",
      |      "columnOrder": 7,
      |      "descriptiveName": "created_date",
      |      "type": "LONG",
      |      "length": 18
      |    },
      |    {
      |      "id": 9614412,
      |      "name": "createdBy",
      |      "created": 1542409377168,
      |      "modified": 1544031975490,
      |      "createdBy": "username",
      |      "modifiedBy": "username",
      |      "description": "This field indicates the user who has created the record.",
      |       "businessColumnName": "createdBy",
      |      "columnOrder": 8,
      |      "type": "STRING",
      |      "length": 50
      |    },
      |    {
      |      "id": 9614413,
      |      "name": "updated_bytxt",
      |      "created": 1542409377169,
      |      "modified": 1544031975490,
      |      "createdBy": "username",
      |      "modifiedBy": "username",
      |      "description": "default description",
      |       "businessColumnName": "updated_bytxt",
      |      "columnOrder": 9,
      |      "descriptiveName": "updated_bytxt",
      |      "type": "STRING",
      |      "length": 50
      |    },
      |    {
      |      "id": 1234,
      |      "name": "updated_date",
      |      "created": 1542409377169,
      |      "modified": 1544031975490,
      |      "createdBy": "username",
      |      "modifiedBy": "username",
      |      "description": " date and time when the record was last updated.",
      |      "businessColumnName": "updated_date",
      |      "columnOrder": 10,
      |      "descriptiveName": "updated_date",
      |      "type": "LONG",
      |      "length": 18
      |
      |    }
      |  ]
      |}
    """.stripMargin

}

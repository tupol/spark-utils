package org.tupol.spark.io.parsers

import com.typesafe.config.ConfigFactory
import org.scalatest.{ FunSuite, Matchers }

import scala.util.Success

class XmlParserConfigurationSpec extends FunSuite with Matchers {

  test("Parse configuration with schema") {

    val configStr = """
                      |format="com.databricks.spark.xml"
                      |path="INPUT_PATH"
                      |rowTag="ROW_TAG"
                      |
                      |schema  : {
                      |  "type" : "struct",
                      |  "fields" : [ {
                      |    "name" : "AddIn",
                      |    "type" : {
                      |      "type" : "struct",
                      |      "fields" : [ {
                      |        "name" : "NetworkRegionalisationAddIn",
                      |        "type" : {
                      |          "type" : "struct",
                      |          "fields" : [ {
                      |            "name" : "EnforceConstrainst",
                      |            "type" : "boolean",
                      |            "nullable" : true,
                      |            "metadata" : { }
                      |          }, {
                      |            "name" : "SDTLogDirectory",
                      |            "type" : "string",
                      |            "nullable" : true,
                      |            "metadata" : { }
                      |          }, {
                      |            "name" : "SDTUpdateDirectory",
                      |            "type" : "string",
                      |            "nullable" : true,
                      |            "metadata" : { }
                      |          }, {
                      |            "name" : "SourceNetworkId",
                      |            "type" : "long",
                      |            "nullable" : true,
                      |            "metadata" : { }
                      |          }, {
                      |            "name" : "XMLLogDirectory",
                      |            "type" : "string",
                      |            "nullable" : true,
                      |            "metadata" : { }
                      |          } ]
                      |        },
                      |        "nullable" : true,
                      |        "metadata" : { }
                      |      }, {
                      |        "name" : "_name",
                      |        "type" : "string",
                      |        "nullable" : true,
                      |        "metadata" : { }
                      |      } ]
                      |    },
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  }, {
                      |    "name" : "Bouquet",
                      |    "type" : {
                      |      "type" : "struct",
                      |      "fields" : [ {
                      |        "name" : "BatDescriptors",
                      |        "type" : "string",
                      |        "nullable" : true,
                      |        "metadata" : { }
                      |      }, {
                      |        "name" : "Id",
                      |        "type" : "long",
                      |        "nullable" : true,
                      |        "metadata" : { }
                      |      }, {
                      |        "name" : "Name",
                      |        "type" : "string",
                      |        "nullable" : true,
                      |        "metadata" : { }
                      |      }, {
                      |        "name" : "TransportStreams",
                      |        "type" : {
                      |          "type" : "struct",
                      |          "fields" : [ {
                      |            "name" : "TransportStream",
                      |            "type" : {
                      |              "type" : "array",
                      |              "elementType" : {
                      |                "type" : "struct",
                      |                "fields" : [ {
                      |                  "name" : "BatDescriptors",
                      |                  "type" : {
                      |                    "type" : "struct",
                      |                    "fields" : [ {
                      |                      "name" : "Descriptor",
                      |                      "type" : {
                      |                        "type" : "array",
                      |                        "elementType" : {
                      |                          "type" : "struct",
                      |                          "fields" : [ {
                      |                            "name" : "Fields",
                      |                            "type" : {
                      |                              "type" : "struct",
                      |                              "fields" : [ {
                      |                                "name" : "Field",
                      |                                "type" : {
                      |                                  "type" : "array",
                      |                                  "elementType" : {
                      |                                    "type" : "struct",
                      |                                    "fields" : [ {
                      |                                      "name" : "Key",
                      |                                      "type" : "string",
                      |                                      "nullable" : true,
                      |                                      "metadata" : { }
                      |                                    }, {
                      |                                      "name" : "Value",
                      |                                      "type" : "string",
                      |                                      "nullable" : true,
                      |                                      "metadata" : { }
                      |                                    } ]
                      |                                  },
                      |                                  "containsNull" : true
                      |                                },
                      |                                "nullable" : true,
                      |                                "metadata" : { }
                      |                              } ]
                      |                            },
                      |                            "nullable" : true,
                      |                            "metadata" : { }
                      |                          }, {
                      |                            "name" : "Name",
                      |                            "type" : "string",
                      |                            "nullable" : true,
                      |                            "metadata" : { }
                      |                          }, {
                      |                            "name" : "RawData",
                      |                            "type" : {
                      |                              "type" : "struct",
                      |                              "fields" : [ {
                      |                                "name" : "_VALUE",
                      |                                "type" : "string",
                      |                                "nullable" : true,
                      |                                "metadata" : { }
                      |                              }, {
                      |                                "name" : "_length",
                      |                                "type" : "long",
                      |                                "nullable" : true,
                      |                                "metadata" : { }
                      |                              } ]
                      |                            },
                      |                            "nullable" : true,
                      |                            "metadata" : { }
                      |                          }, {
                      |                            "name" : "Tag",
                      |                            "type" : "long",
                      |                            "nullable" : true,
                      |                            "metadata" : { }
                      |                          } ]
                      |                        },
                      |                        "containsNull" : true
                      |                      },
                      |                      "nullable" : true,
                      |                      "metadata" : { }
                      |                    } ]
                      |                  },
                      |                  "nullable" : true,
                      |                  "metadata" : { }
                      |                }, {
                      |                  "name" : "Id",
                      |                  "type" : "long",
                      |                  "nullable" : true,
                      |                  "metadata" : { }
                      |                }, {
                      |                  "name" : "Name",
                      |                  "type" : "string",
                      |                  "nullable" : true,
                      |                  "metadata" : { }
                      |                }, {
                      |                  "name" : "OriginalNetworkId",
                      |                  "type" : "long",
                      |                  "nullable" : true,
                      |                  "metadata" : { }
                      |                } ]
                      |              },
                      |              "containsNull" : true
                      |            },
                      |            "nullable" : true,
                      |            "metadata" : { }
                      |          } ]
                      |        },
                      |        "nullable" : true,
                      |        "metadata" : { }
                      |      } ]
                      |    },
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  }, {
                      |    "name" : "Network",
                      |    "type" : {
                      |      "type" : "struct",
                      |      "fields" : [ {
                      |        "name" : "AddIn",
                      |        "type" : {
                      |          "type" : "struct",
                      |          "fields" : [ {
                      |            "name" : "NetworkRegionalisationAddIn",
                      |            "type" : {
                      |              "type" : "struct",
                      |              "fields" : [ {
                      |                "name" : "RegionalizedTransportStream",
                      |                "type" : {
                      |                  "type" : "array",
                      |                  "elementType" : {
                      |                    "type" : "struct",
                      |                    "fields" : [ {
                      |                      "name" : "_VALUE",
                      |                      "type" : "long",
                      |                      "nullable" : true,
                      |                      "metadata" : { }
                      |                    }, {
                      |                      "name" : "_onid",
                      |                      "type" : "long",
                      |                      "nullable" : true,
                      |                      "metadata" : { }
                      |                    }, {
                      |                      "name" : "_tsid",
                      |                      "type" : "long",
                      |                      "nullable" : true,
                      |                      "metadata" : { }
                      |                    } ]
                      |                  },
                      |                  "containsNull" : true
                      |                },
                      |                "nullable" : true,
                      |                "metadata" : { }
                      |              } ]
                      |            },
                      |            "nullable" : true,
                      |            "metadata" : { }
                      |          }, {
                      |            "name" : "_name",
                      |            "type" : "string",
                      |            "nullable" : true,
                      |            "metadata" : { }
                      |          } ]
                      |        },
                      |        "nullable" : true,
                      |        "metadata" : { }
                      |      }, {
                      |        "name" : "Id",
                      |        "type" : "long",
                      |        "nullable" : true,
                      |        "metadata" : { }
                      |      }, {
                      |        "name" : "Name",
                      |        "type" : "string",
                      |        "nullable" : true,
                      |        "metadata" : { }
                      |      }, {
                      |        "name" : "NitDescriptors",
                      |        "type" : "string",
                      |        "nullable" : true,
                      |        "metadata" : { }
                      |      }, {
                      |        "name" : "TransportStreams",
                      |        "type" : {
                      |          "type" : "struct",
                      |          "fields" : [ {
                      |            "name" : "TransportStream",
                      |            "type" : {
                      |              "type" : "array",
                      |              "elementType" : {
                      |                "type" : "struct",
                      |                "fields" : [ {
                      |                  "name" : "CatDescriptors",
                      |                  "type" : "string",
                      |                  "nullable" : true,
                      |                  "metadata" : { }
                      |                }, {
                      |                  "name" : "Id",
                      |                  "type" : "long",
                      |                  "nullable" : true,
                      |                  "metadata" : { }
                      |                }, {
                      |                  "name" : "Name",
                      |                  "type" : "string",
                      |                  "nullable" : true,
                      |                  "metadata" : { }
                      |                }, {
                      |                  "name" : "OriginalNetworks",
                      |                  "type" : {
                      |                    "type" : "struct",
                      |                    "fields" : [ {
                      |                      "name" : "OriginalNetwork",
                      |                      "type" : {
                      |                        "type" : "array",
                      |                        "elementType" : {
                      |                          "type" : "struct",
                      |                          "fields" : [ {
                      |                            "name" : "Id",
                      |                            "type" : "long",
                      |                            "nullable" : true,
                      |                            "metadata" : { }
                      |                          }, {
                      |                            "name" : "NitDescriptors",
                      |                            "type" : "string",
                      |                            "nullable" : true,
                      |                            "metadata" : { }
                      |                          }, {
                      |                            "name" : "Services",
                      |                            "type" : {
                      |                              "type" : "struct",
                      |                              "fields" : [ {
                      |                                "name" : "Service",
                      |                                "type" : {
                      |                                  "type" : "array",
                      |                                  "elementType" : {
                      |                                    "type" : "struct",
                      |                                    "fields" : [ {
                      |                                      "name" : "AddIn",
                      |                                      "type" : {
                      |                                        "type" : "struct",
                      |                                        "fields" : [ {
                      |                                          "name" : "StagisDvbAddIn",
                      |                                          "type" : {
                      |                                            "type" : "struct",
                      |                                            "fields" : [ {
                      |                                              "name" : "Routing",
                      |                                              "type" : {
                      |                                                "type" : "struct",
                      |                                                "fields" : [ {
                      |                                                  "name" : "EpgInput",
                      |                                                  "type" : "string",
                      |                                                  "nullable" : true,
                      |                                                  "metadata" : { }
                      |                                                } ]
                      |                                              },
                      |                                              "nullable" : true,
                      |                                              "metadata" : { }
                      |                                            } ]
                      |                                          },
                      |                                          "nullable" : true,
                      |                                          "metadata" : { }
                      |                                        }, {
                      |                                          "name" : "_name",
                      |                                          "type" : "string",
                      |                                          "nullable" : true,
                      |                                          "metadata" : { }
                      |                                        } ]
                      |                                      },
                      |                                      "nullable" : true,
                      |                                      "metadata" : { }
                      |                                    }, {
                      |                                      "name" : "Id",
                      |                                      "type" : "long",
                      |                                      "nullable" : true,
                      |                                      "metadata" : { }
                      |                                    }, {
                      |                                      "name" : "Name",
                      |                                      "type" : "string",
                      |                                      "nullable" : true,
                      |                                      "metadata" : { }
                      |                                    }, {
                      |                                      "name" : "PmtPid",
                      |                                      "type" : "long",
                      |                                      "nullable" : true,
                      |                                      "metadata" : { }
                      |                                    }, {
                      |                                      "name" : "SdtDescriptors",
                      |                                      "type" : {
                      |                                        "type" : "struct",
                      |                                        "fields" : [ {
                      |                                          "name" : "Descriptor",
                      |                                          "type" : {
                      |                                            "type" : "array",
                      |                                            "elementType" : {
                      |                                              "type" : "struct",
                      |                                              "fields" : [ {
                      |                                                "name" : "Fields",
                      |                                                "type" : {
                      |                                                  "type" : "struct",
                      |                                                  "fields" : [ {
                      |                                                    "name" : "Field",
                      |                                                    "type" : {
                      |                                                      "type" : "array",
                      |                                                      "elementType" : {
                      |                                                        "type" : "struct",
                      |                                                        "fields" : [ {
                      |                                                          "name" : "Key",
                      |                                                          "type" : "string",
                      |                                                          "nullable" : true,
                      |                                                          "metadata" : { }
                      |                                                        }, {
                      |                                                          "name" : "Value",
                      |                                                          "type" : "long",
                      |                                                          "nullable" : true,
                      |                                                          "metadata" : { }
                      |                                                        } ]
                      |                                                      },
                      |                                                      "containsNull" : true
                      |                                                    },
                      |                                                    "nullable" : true,
                      |                                                    "metadata" : { }
                      |                                                  } ]
                      |                                                },
                      |                                                "nullable" : true,
                      |                                                "metadata" : { }
                      |                                              }, {
                      |                                                "name" : "Name",
                      |                                                "type" : "string",
                      |                                                "nullable" : true,
                      |                                                "metadata" : { }
                      |                                              }, {
                      |                                                "name" : "RawData",
                      |                                                "type" : {
                      |                                                  "type" : "struct",
                      |                                                  "fields" : [ {
                      |                                                    "name" : "_VALUE",
                      |                                                    "type" : "string",
                      |                                                    "nullable" : true,
                      |                                                    "metadata" : { }
                      |                                                  }, {
                      |                                                    "name" : "_length",
                      |                                                    "type" : "long",
                      |                                                    "nullable" : true,
                      |                                                    "metadata" : { }
                      |                                                  } ]
                      |                                                },
                      |                                                "nullable" : true,
                      |                                                "metadata" : { }
                      |                                              }, {
                      |                                                "name" : "Tag",
                      |                                                "type" : "long",
                      |                                                "nullable" : true,
                      |                                                "metadata" : { }
                      |                                              } ]
                      |                                            },
                      |                                            "containsNull" : true
                      |                                          },
                      |                                          "nullable" : true,
                      |                                          "metadata" : { }
                      |                                        } ]
                      |                                      },
                      |                                      "nullable" : true,
                      |                                      "metadata" : { }
                      |                                    }, {
                      |                                      "name" : "Type",
                      |                                      "type" : {
                      |                                        "type" : "struct",
                      |                                        "fields" : [ {
                      |                                          "name" : "_VALUE",
                      |                                          "type" : "string",
                      |                                          "nullable" : true,
                      |                                          "metadata" : { }
                      |                                        }, {
                      |                                          "name" : "_type",
                      |                                          "type" : "long",
                      |                                          "nullable" : true,
                      |                                          "metadata" : { }
                      |                                        } ]
                      |                                      },
                      |                                      "nullable" : true,
                      |                                      "metadata" : { }
                      |                                    } ]
                      |                                  },
                      |                                  "containsNull" : true
                      |                                },
                      |                                "nullable" : true,
                      |                                "metadata" : { }
                      |                              } ]
                      |                            },
                      |                            "nullable" : true,
                      |                            "metadata" : { }
                      |                          } ]
                      |                        },
                      |                        "containsNull" : true
                      |                      },
                      |                      "nullable" : true,
                      |                      "metadata" : { }
                      |                    } ]
                      |                  },
                      |                  "nullable" : true,
                      |                  "metadata" : { }
                      |                }, {
                      |                  "name" : "TotDescriptors",
                      |                  "type" : "string",
                      |                  "nullable" : true,
                      |                  "metadata" : { }
                      |                } ]
                      |              },
                      |              "containsNull" : true
                      |            },
                      |            "nullable" : true,
                      |            "metadata" : { }
                      |          } ]
                      |        },
                      |        "nullable" : true,
                      |        "metadata" : { }
                      |      } ]
                      |    },
                      |    "nullable" : true,
                      |    "metadata" : { }
                      |  } ]
                      |}
                      |
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)

    val parserConfig = ParserConfiguration(config)

    parserConfig shouldBe a[Success[_]]

    parserConfig.get shouldBe an[XmlParserConfiguration]

    parserConfig.get.schema shouldBe a[Some[_]]

  }

  test("Parse configuration without schema") {

    val configStr = """
                      |format="com.databricks.spark.xml"
                      |path="INPUT_PATH"
                      |rowTag="ROW_TAG"
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)

    val parserConfig = ParserConfiguration(config)

    parserConfig shouldBe a[Success[_]]

    parserConfig.get shouldBe an[XmlParserConfiguration]

    parserConfig.get.schema.isDefined shouldBe false

    parserConfig.get.parserOptions shouldBe Map("rowTag" -> "ROW_TAG")

  }

  test("Parse configuration with parserOptions") {

    val configStr = """
                      |format="com.databricks.spark.xml"
                      |path="INPUT_PATH"
                      |rowTag="ROW_TAG"
                      |parserOptions=[
                      |   {"mode" : "PERMISSIVE"},
                      |   {"samplingRatio" : "1"},
                      |   {"charset" : "UTF-8"}
                      |]
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)

    val parserConfig = ParserConfiguration(config)

    parserConfig shouldBe a[Success[_]]

    parserConfig.get shouldBe an[XmlParserConfiguration]

    parserConfig.get.parserOptions.isEmpty shouldBe false

    parserConfig.get.parserOptions should contain
    theSameElementsAs(Map("mode" -> "PERMISSIVE", "samplingRatio" -> "1", "charset" -> "UTF-8"))

  }

  test("Parse configuration with parserOptions overridden by top level properties") {

    val configStr = """
                      |format="com.databricks.spark.xml"
                      |path="INPUT_PATH"
                      |rowTag="ROW_TAG"
                      |parserOptions=[
                      |   {"mode" : "PERMISSIVE"},
                      |   {"samplingRatio" : "1"},
                      |   {"charset" : "UTF-8"},
                      |   {"rowTag": "INNER_ROW_TAG"}
                      |]
                    """.stripMargin

    val config = ConfigFactory.parseString(configStr)

    val parserConfig = ParserConfiguration(config)

    parserConfig shouldBe a[Success[_]]

    parserConfig.get shouldBe an[XmlParserConfiguration]

    parserConfig.get.parserOptions.isEmpty shouldBe false

    parserConfig.get.parserOptions should be
    Map("mode" -> "PERMISSIVE", "samplingRatio" -> "1", "charset" -> "UTF-8", "rowTag" -> "ROW_TAG")

  }

}

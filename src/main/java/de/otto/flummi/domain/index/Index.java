package de.otto.flummi.domain.index;

public class Index {
    private Settings settings = new Settings();

    Settings getSettings() {
        return settings;
    }
    /*
{
  "aliases": {
    "suggest-searchstatistics": {}
  },
  "settings": {
    "number_of_shards": 1
  },
  "mappings": {
    "searchstatistics": {
      "properties": {
        "field1": {
          "type": "text"
        }
      }
    }
  }
}
*/
}

[![Build Status](https://travis-ci.org/pbuda/akka-persistence-file-journal.svg)](https://travis-ci.org/pbuda/akka-persistence-file-journal)

akka-persistence-file-journal
=============================

File Journal for Akka Persistence


# File format description
 
## Meta file

The `meta` file is a special file which contains some information regarding `data` file. It's used to speed up
lookup of the messages stored in `data` file. At first I was considering putting the metadata in the `data` file's
header, but the growing nature of the metadata would require constant reformatting of the data file which would be
time consuming operation.

### Format

#### Meta file's header

| Offset | Field name | Data Type | Length      | Description                                                 |
| -----: | ---------- | --------- | ----------- | ----------------------------------------------------------- |
| 0      | Header     | String    | 8 bytes     | A string literal `APFM`                                     |
| 8      | Meta size  | Long      | 8 bytes     | The total size of the `meta` blocks                         |
| 16     | Metadata   | Blocks    | Unspecified | The blocks with information about messages from `data` file |

Metadata blocks are what is the most important part of the `meta` file. They contain information regarding messages
for persistence ids. Each block represents metadata of a single persistence id.

#### Meta file's metadata block

The metadata blocks have the following structure:

| Field name                  | Length   | Description                                                        |
| --------------------------- | -------- | ------------------------------------------------------------------ |
| Offset of the first message | 8 bytes  | Used to find the first message in the `data` file                  |
| Offset of the last message  | 8 bytes  | Used to find the last message in the `data` file                   |
| Highest sequence number     | 8 bytes  | The highest sequence number of the messages for the persistence id |
| Persistence Id              | Variable | Persistence id of the actor                                        |



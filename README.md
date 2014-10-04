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
| 0      | Header     | String    | 4 bytes     | UTF-8 encoded string literal `APFM`                         |
| 4      | Meta size  | Int       | 4 bytes     | The total size of the `meta` blocks                         |
| 8      | Metadata   | Blocks    | Unspecified | The blocks with information about messages from `data` file |

#### Meta file's metadata block

Metadata blocks are what is the most important part of the `meta` file. They contain information regarding messages
for persistence ids. Each block represents metadata of a single persistence id.

The metadata blocks have the following structure:

| Field name                  | Length   | Description                                                        |
| --------------------------- | -------- | ------------------------------------------------------------------ |
| Block's size                | 2 bytes  | Indicates how big this block is                                    |
| Offset of the first message | 8 bytes  | Used to find the first message in the `data` file                  |
| Offset of the last message  | 8 bytes  | Used to find the last message in the `data` file                   |
| Highest sequence number     | 8 bytes  | The highest sequence number of the messages for the persistence id |
| Persistence Id              | Variable | UTF-8 encoded persistence id of the actor                          |



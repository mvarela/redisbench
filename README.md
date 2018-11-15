# Naive benchmarking...

Here are some parts of benchmark runs. Overall, the write performance seems to
be unaltered by the number of keys, key size, or payload size. However, reads
seem consistently slow, and so are deletes.


 With 10KB payloads, and 400K keys in the DB / key multiplier 1 / deleting slowly:
```
 18-11-15 16:15:55 Lambdabeast2 INFO [redisbench.core:70] - Added 59338, deleted 11 - 399977 keys in db
 18-11-15 16:15:56 Lambdabeast2 INFO [redisbench.core:70] - Added 59329, deleted 13 - 399973 keys in db
 18-11-15 16:15:57 Lambdabeast2 INFO [redisbench.core:70] - Added 59559, deleted 14 - 399972 keys in db
 18-11-15 16:15:58 Lambdabeast2 INFO [redisbench.core:70] - Added 59829, deleted 13 - 399971 keys in db
 18-11-15 16:15:59 Lambdabeast2 INFO [redisbench.core:70] - Added 59940, deleted 11 - 399971 keys in db
 18-11-15 16:16:00 Lambdabeast2 INFO [redisbench.core:70] - Added 59140, deleted 11 - 399977 keys in db
 18-11-15 16:16:01 Lambdabeast2 INFO [redisbench.core:70] - Added 58855, deleted 15 - 399977 keys in db
 18-11-15 16:16:02 Lambdabeast2 INFO [redisbench.core:70] - Added 58396, deleted 11 - 399980 keys in db
 18-11-15 16:16:03 Lambdabeast2 INFO [redisbench.core:70] - Added 59422, deleted 14 - 399979 keys in db
 18-11-15 16:16:04 Lambdabeast2 INFO [redisbench.core:70] - Added 59014, deleted 15 - 399971 keys in db
```

 With 100KB payloads, and 400K keys in the DB / key multiplier 5 / deleting slowly:
```
 18-11-15 16:19:02 Lambdabeast2 INFO [redisbench.core:70] - Added 57743, deleted 12 - 93943 keys in db
 18-11-15 16:19:03 Lambdabeast2 INFO [redisbench.core:70] - Added 58307, deleted 13 - 96596 keys in db
 18-11-15 16:19:04 Lambdabeast2 INFO [redisbench.core:70] - Added 57114, deleted 12 - 98040 keys in db
 18-11-15 16:19:05 Lambdabeast2 INFO [redisbench.core:70] - Added 57440, deleted 13 - 98865 keys in db
 18-11-15 16:19:06 Lambdabeast2 INFO [redisbench.core:70] - Added 57660, deleted 12 - 99356 keys in db
 18-11-15 16:19:07 Lambdabeast2 INFO [redisbench.core:70] - Added 58804, deleted 10 - 99618 keys in db
 18-11-15 16:19:08 Lambdabeast2 INFO [redisbench.core:70] - Added 58278, deleted 12 - 99783 keys in db
 18-11-15 16:19:09 Lambdabeast2 INFO [redisbench.core:70] - Added 57785, deleted 12 - 99880 keys in db
 18-11-15 16:19:10 Lambdabeast2 INFO [redisbench.core:70] - Added 57570, deleted 12 - 99924 keys in db
 18-11-15 16:19:11 Lambdabeast2 INFO [redisbench.core:70] - Added 57896, deleted 13 - 99943 keys in db
 ```


 With 100KB payloads, and ~600K keys in the DB / key multiplier 20 (720B/key) / deleting continuously:

 ```
 18-11-15 16:27:07 Lambdabeast2 INFO [redisbench.core:70] - Added 55672, deleted 5 - 590512 keys in db
 18-11-15 16:27:08 Lambdabeast2 INFO [redisbench.core:70] - Added 54371, deleted 6 - 591467 keys in db
 18-11-15 16:27:09 Lambdabeast2 INFO [redisbench.core:70] - Added 54479, deleted 6 - 592337 keys in db
 18-11-15 16:27:10 Lambdabeast2 INFO [redisbench.core:70] - Added 53327, deleted 5 - 593082 keys in db
 18-11-15 16:27:11 Lambdabeast2 INFO [redisbench.core:70] - Added 48199, deleted 5 - 593706 keys in db
 18-11-15 16:27:12 Lambdabeast2 INFO [redisbench.core:70] - Added 49125, deleted 6 - 594253 keys in db
 18-11-15 16:27:13 Lambdabeast2 INFO [redisbench.core:70] - Added 48813, deleted 5 - 594785 keys in db
 18-11-15 16:27:14 Lambdabeast2 INFO [redisbench.core:70] - Added 46417, deleted 5 - 595252 keys in db
 18-11-15 16:27:15 Lambdabeast2 INFO [redisbench.core:70] - Added 50403, deleted 6 - 595665 keys in db
 18-11-15 16:27:16 Lambdabeast2 INFO [redisbench.core:70] - Added 53315, deleted 5 - 596091 keys in db
 18-11-15 16:27:17 Lambdabeast2 INFO [redisbench.core:70] - Added 55882, deleted 5 - 596488 keys in db
 18-11-15 16:27:18 Lambdabeast2 INFO [redisbench.core:70] - Added 55621, deleted 5 - 596847 keys in db
 18-11-15 16:27:19 Lambdabeast2 INFO [redisbench.core:70] - Added 52297, deleted 6 - 597159 keys in db
 18-11-15 16:27:20 Lambdabeast2 INFO [redisbench.core:70] - Added 48103, deleted 6 - 597395 keys in db
 18-11-15 16:27:21 Lambdabeast2 INFO [redisbench.core:70] - Added 48542, deleted 5 - 597631 keys in db
 18-11-15 16:27:22 Lambdabeast2 INFO [redisbench.core:70] - Added 45249, deleted 5 - 597852 keys in db
 18-11-15 16:27:23 Lambdabeast2 INFO [redisbench.core:70] - Added 50866, deleted 6 - 598050 keys in db
 18-11-15 16:27:24 Lambdabeast2 INFO [redisbench.core:70] - Added 53948, deleted 5 - 598237 keys in db
 18-11-15 16:27:25 Lambdabeast2 INFO [redisbench.core:70] - Added 52206, deleted 5 - 598394 keys in db
 ```

 Note the slowing down of key deletion with the larger keyspace
 Using car/del is O1 in the number of keys, and On on the object size.
 Using car/unkink doesn't improve the situation.

 Adding reader threads, it gets worse:
 With 100KB payloads, and ~500K keys in the DB / key multiplier 2  / deleting continuously:
 ```
 18-11-15 16:53:48 Lambdabeast2 INFO [redisbench.core:70] - Added 22998, read 31, deleted 4 - 477306 keys in db
 18-11-15 16:53:49 Lambdabeast2 INFO [redisbench.core:70] - Added 21477, read 30, deleted 4 - 478282 keys in db
 18-11-15 16:53:50 Lambdabeast2 INFO [redisbench.core:70] - Added 21225, read 30, deleted 4 - 479202 keys in db
 18-11-15 16:53:51 Lambdabeast2 INFO [redisbench.core:70] - Added 20548, read 30, deleted 3 - 480020 keys in db
 18-11-15 16:53:52 Lambdabeast2 INFO [redisbench.core:70] - Added 20944, read 30, deleted 4 - 480814 keys in db
 18-11-15 16:53:53 Lambdabeast2 INFO [redisbench.core:70] - Added 21298, read 30, deleted 4 - 481642 keys in db
 18-11-15 16:53:54 Lambdabeast2 INFO [redisbench.core:70] - Added 21407, read 30, deleted 4 - 482422 keys in db
 18-11-15 16:53:55 Lambdabeast2 INFO [redisbench.core:70] - Added 20733, read 26, deleted 3 - 483109 keys in db
 18-11-15 16:53:56 Lambdabeast2 INFO [redisbench.core:70] - Added 20381, read 32, deleted 4 - 483737 keys in db
 18-11-15 16:53:57 Lambdabeast2 INFO [redisbench.core:70] - Added 22243, read 35, deleted 3 - 484454 keys in db
 18-11-15 16:53:58 Lambdabeast2 INFO [redisbench.core:70] - Added 22361, read 35, deleted 4 - 485099 keys in db
 18-11-15 16:53:59 Lambdabeast2 INFO [redisbench.core:70] - Added 22393, read 29, deleted 3 - 485750 keys in db
 18-11-15 16:54:00 Lambdabeast2 INFO [redisbench.core:70] - Added 23045, read 34, deleted 4 - 486399 keys in db
 18-11-15 16:54:01 Lambdabeast2 INFO [redisbench.core:70] - Added 20565, read 30, deleted 4 - 486955 keys in db
 18-11-15 16:54:02 Lambdabeast2 INFO [redisbench.core:70] - Added 20709, read 31, deleted 3 - 487455 keys in db
 18-11-15 16:54:03 Lambdabeast2 INFO [redisbench.core:70] - Added 22952, read 29, deleted 4 - 488042 keys in db
 18-11-15 16:54:04 Lambdabeast2 INFO [redisbench.core:70] - Added 21463, read 30, deleted 4 - 488536 keys in db
 18-11-15 16:54:05 Lambdabeast2 INFO [redisbench.core:70] - Added 23602, read 30, deleted 3 - 489083 keys in db
 18-11-15 16:54:06 Lambdabeast2 INFO [redisbench.core:70] - Added 20721, read 35, deleted 4 - 489532 keys in db
 18-11-15 16:54:07 Lambdabeast2 INFO [redisbench.core:70] - Added 21044, read 26, deleted 3 - 489963 keys in db
 18-11-15 16:54:08 Lambdabeast2 INFO [redisbench.core:70] - Added 21735, read 30, deleted 4 - 490396 keys in db
 18-11-15 16:54:09 Lambdabeast2 INFO [redisbench.core:70] - Added 22323, read 30, deleted 3 - 490795 keys in db
 18-11-15 16:54:10 Lambdabeast2 INFO [redisbench.core:70] - Added 22129, read 34, deleted 4 - 491193 keys in db
 ```

 Even with small payload size, read performance seems to decrease
 ```
 18-11-15 16:56:36 Lambdabeast2 INFO [redisbench.core:70] - Added 5, read 0, deleted 0 - 4 keys in db
 18-11-15 16:56:37 Lambdabeast2 INFO [redisbench.core:70] - Added 26237, read 620, deleted 1 - 25811 keys in db
 18-11-15 16:56:38 Lambdabeast2 INFO [redisbench.core:70] - Added 22662, read 458, deleted 9 - 46829 keys in db
 18-11-15 16:56:39 Lambdabeast2 INFO [redisbench.core:70] - Added 23824, read 270, deleted 9 - 67920 keys in db
 18-11-15 16:56:40 Lambdabeast2 INFO [redisbench.core:70] - Added 23974, read 204, deleted 8 - 88301 keys in db
 18-11-15 16:56:41 Lambdabeast2 INFO [redisbench.core:70] - Added 24500, read 174, deleted 7 - 108003 keys in db
 18-11-15 16:56:42 Lambdabeast2 INFO [redisbench.core:70] - Added 22981, read 138, deleted 8 - 125623 keys in db
 18-11-15 16:56:43 Lambdabeast2 INFO [redisbench.core:70] - Added 22488, read 125, deleted 6 - 142086 keys in db
 18-11-15 16:56:44 Lambdabeast2 INFO [redisbench.core:70] - Added 24378, read 115, deleted 6 - 159175 keys in db
 18-11-15 16:56:45 Lambdabeast2 INFO [redisbench.core:70] - Added 23615, read 109, deleted 6 - 174946 keys in db
 18-11-15 16:56:46 Lambdabeast2 INFO [redisbench.core:70] - Added 23580, read 100, deleted 6 - 189832 keys in db
 18-11-15 16:56:47 Lambdabeast2 INFO [redisbench.core:70] - Added 23676, read 95, deleted 6 - 204046 keys in db
 18-11-15 16:56:48 Lambdabeast2 INFO [redisbench.core:70] - Added 22765, read 80, deleted 6 - 217147 keys in db
 18-11-15 16:56:49 Lambdabeast2 INFO [redisbench.core:70] - Added 23851, read 71, deleted 6 - 230411 keys in db
 18-11-15 16:56:50 Lambdabeast2 INFO [redisbench.core:70] - Added 22339, read 75, deleted 5 - 242176 keys in db
 18-11-15 16:56:51 Lambdabeast2 INFO [redisbench.core:70] - Added 22538, read 72, deleted 6 - 253635 keys in db
 18-11-15 16:56:52 Lambdabeast2 INFO [redisbench.core:70] - Added 22656, read 66, deleted 5 - 264633 keys in db
 18-11-15 16:56:53 Lambdabeast2 INFO [redisbench.core:70] - Added 21701, read 70, deleted 4 - 274523 keys in db
 18-11-15 16:56:54 Lambdabeast2 INFO [redisbench.core:70] - Added 22399, read 57, deleted 5 - 284476 keys in db
 18-11-15 16:56:55 Lambdabeast2 INFO [redisbench.core:70] - Added 22587, read 59, deleted 5 - 294113 keys in db
 18-11-15 16:56:56 Lambdabeast2 INFO [redisbench.core:70] - Added 23568, read 60, deleted 5 - 303636 keys in db
 18-11-15 16:56:57 Lambdabeast2 INFO [redisbench.core:70] - Added 22145, read 60, deleted 5 - 312049 keys in db
 18-11-15 16:56:58 Lambdabeast2 INFO [redisbench.core:70] - Added 19956, read 56, deleted 5 - 319332 keys in db
 18-11-15 16:56:59 Lambdabeast2 INFO [redisbench.core:70] - Added 22860, read 51, deleted 5 - 327423 keys in db
 18-11-15 16:57:00 Lambdabeast2 INFO [redisbench.core:70] - Added 21740, read 46, deleted 5 - 334709 keys in db
 18-11-15 16:57:01 Lambdabeast2 INFO [redisbench.core:70] - Added 19606, read 50, deleted 5 - 341051 keys in db
 18-11-15 16:57:02 Lambdabeast2 INFO [redisbench.core:70] - Added 21104, read 46, deleted 4 - 347560 keys in db
 18-11-15 16:57:03 Lambdabeast2 INFO [redisbench.core:70] - Added 21247, read 47, deleted 5 - 353921 keys in db
 18-11-15 16:57:04 Lambdabeast2 INFO [redisbench.core:70] - Added 21905, read 49, deleted 4 - 360221 keys in db
 18-11-15 16:57:05 Lambdabeast2 INFO [redisbench.core:70] - Added 21862, read 41, deleted 5 - 366111 keys in db
 18-11-15 16:57:06 Lambdabeast2 INFO [redisbench.core:70] - Added 21791, read 45, deleted 5 - 371786 keys in db
 18-11-15 16:57:07 Lambdabeast2 INFO [redisbench.core:70] - Added 21809, read 45, deleted 4 - 377145 keys in db
 18-11-15 16:57:08 Lambdabeast2 INFO [redisbench.core:70] - Added 21243, read 45, deleted 4 - 382297 keys in db
 ```
If we limit the write rate (1000/s), we can see the read performance decrease
with the number of keys:
```
18-11-15 17:19:40 Lambdabeast2 INFO [redisbench.core:71] - Added 2, read 0, deleted 0 - 0 keys in db
18-11-15 17:19:42 Lambdabeast2 INFO [redisbench.core:71] - Added 1000, read 9918, deleted 1 - 1211 keys in db
18-11-15 17:19:43 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 10019, deleted 10 - 2200 keys in db
18-11-15 17:19:44 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 7215, deleted 10 - 3182 keys in db
18-11-15 17:19:45 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 5138, deleted 10 - 4167 keys in db
18-11-15 17:19:46 Lambdabeast2 INFO [redisbench.core:71] - Added 1000, read 4504, deleted 10 - 5145 keys in db
18-11-15 17:19:47 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 3865, deleted 10 - 6127 keys in db
18-11-15 17:19:48 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 3343, deleted 10 - 7106 keys in db
18-11-15 17:19:49 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 2965, deleted 10 - 8080 keys in db
18-11-15 17:19:50 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 2631, deleted 9 - 9056 keys in db
18-11-15 17:19:51 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 2395, deleted 10 - 10028 keys in db
18-11-15 17:19:52 Lambdabeast2 INFO [redisbench.core:71] - Added 1000, read 2226, deleted 10 - 10990 keys in db
18-11-15 17:19:53 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 1977, deleted 10 - 11956 keys in db
18-11-15 17:19:54 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 1707, deleted 10 - 12915 keys in db
18-11-15 17:19:55 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 1634, deleted 9 - 13878 keys in db
18-11-15 17:19:56 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 1445, deleted 10 - 14845 keys in db
18-11-15 17:19:57 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 1221, deleted 10 - 15806 keys in db
18-11-15 17:19:58 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 1349, deleted 9 - 16770 keys in db
18-11-15 17:19:59 Lambdabeast2 INFO [redisbench.core:71] - Added 1000, read 1299, deleted 10 - 17725 keys in db
18-11-15 17:20:00 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 1052, deleted 10 - 18672 keys in db
18-11-15 17:20:01 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 1109, deleted 9 - 19626 keys in db
18-11-15 17:20:02 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 899, deleted 10 - 20579 keys in db
18-11-15 17:20:03 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 829, deleted 9 - 21536 keys in db
18-11-15 17:20:04 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 772, deleted 10 - 22484 keys in db
18-11-15 17:20:05 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 796, deleted 9 - 23423 keys in db
18-11-15 17:20:06 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 661, deleted 10 - 24369 keys in db
18-11-15 17:20:07 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 616, deleted 9 - 25303 keys in db
18-11-15 17:20:08 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 656, deleted 10 - 26242 keys in db
18-11-15 17:20:09 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 550, deleted 9 - 27182 keys in db
18-11-15 17:20:10 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 524, deleted 9 - 28121 keys in db
18-11-15 17:20:11 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 461, deleted 9 - 29054 keys in db
18-11-15 17:20:12 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 422, deleted 10 - 29989 keys in db
18-11-15 17:20:13 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 474, deleted 9 - 30921 keys in db
18-11-15 17:20:14 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 439, deleted 9 - 31853 keys in db
18-11-15 17:20:15 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 422, deleted 9 - 32777 keys in db
18-11-15 17:20:16 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 411, deleted 9 - 33696 keys in db
18-11-15 17:20:17 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 397, deleted 9 - 34622 keys in db
18-11-15 17:20:18 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 352, deleted 9 - 35543 keys in db
18-11-15 17:20:19 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 326, deleted 10 - 36442 keys in db
18-11-15 17:20:20 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 316, deleted 9 - 37356 keys in db
18-11-15 17:20:21 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 368, deleted 9 - 38273 keys in db
18-11-15 17:20:22 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 378, deleted 9 - 39197 keys in db
18-11-15 17:20:23 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 304, deleted 8 - 40114 keys in db
18-11-15 17:20:24 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 327, deleted 9 - 41016 keys in db
18-11-15 17:20:25 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 319, deleted 9 - 41934 keys in db
18-11-15 17:20:26 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 315, deleted 9 - 42848 keys in db
18-11-15 17:20:27 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 303, deleted 9 - 43753 keys in db
18-11-15 17:20:28 Lambdabeast2 INFO [redisbench.core:71] - Added 1000, read 324, deleted 9 - 44640 keys in db
18-11-15 17:20:29 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 292, deleted 9 - 45544 keys in db
18-11-15 17:20:30 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 287, deleted 9 - 46429 keys in db
18-11-15 17:20:31 Lambdabeast2 INFO [redisbench.core:71] - Added 1000, read 248, deleted 8 - 47326 keys in db
18-11-15 17:20:32 Lambdabeast2 INFO [redisbench.core:71] - Added 1001, read 235, deleted 9 - 48219 keys in db
```

Overall, what this tells me is that:
1 - The number of keys is an issue 
2 - payload size is not important
3 - writes are always fast
4 - reads/deletes are super slow

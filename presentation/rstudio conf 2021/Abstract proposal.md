Learn how to handle 100GBs of data with ease using {disk.frame} - the larger-than-RAM-data manipulation package.

R loads data in its entirety into RAM. However, RAM is a precious resource and often do run out. That's why most R user would have run into the "cannot allocate vector of size xxB." error at some point.

However, the need to handle larger-than-RAM data doesn't go away just because RAM isn't large enough. So many useRs turn to big data tools like Spark for the task. In this talk, I will make the case that {disk.frame} is sufficient and often preferable for manipulating larger-than-RAM data that fit on disk. I will show how you can apply familiar {dplyr}-verbs to manipulate larger-than-RAM data with {disk.frame}.
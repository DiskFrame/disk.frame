# Spec
A disk.frame is a folder containing 

* chunks: a list of files, where each file is called a chunk, with names in the form "d.ext" where d is a positive integer starting from 1 and  ext is the extension and fst being the predominant extension
* Optional: a sub-folder called .metadata

If RAM is large enough then loading each chunk into RAM and row-binding all the chunks should give a `data.frame`. 

# The .metadata folder

The .metadata folder is designed to hold arbitrary data regarding the data.frame. E.g. bloomfilters can be implemented in the .metadata folder.

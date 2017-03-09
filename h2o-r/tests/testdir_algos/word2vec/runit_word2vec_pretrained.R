# import pre-trained model into an H2O frame
pretrained.frame <- h2o.importFile("/Users/mkurka/Downloads/glove/glove.twitter.27B.100d.txt")

# convert to an H2O word2vec model
pretrained.w2v <- h2o.word2vec(pre_trained = pretrained.frame, vec_size = 100)

# test it
h2o.findSynonyms(pretrained.w2v, "car", count = 5)

# mojo for a Glove model :)
h2o.download_mojo(pretrained.w2v)
(lang dune 2.7)
(name kv_hash)

;; for opam
(authors              "Tom Ridge <tom.j.ridge@googlemail.com>")
(maintainers          "Tom Ridge <tom.j.ridge@googlemail.com>")
(source               (github tomjridge/FIXME))
(homepage             https://github.com/tomjridge/FIXME)
(documentation        https://tomjridge.github.io/FIXME/ocamldoc/index.html)
(generate_opam_files  true)


(package 
 (name kv_hash)
 (synopsis "A Key Value store using a persistent hashtable")
 (depends core xxhash ctypes lru extunix logs)) ; core.bigstring_unix - a lib provided by core



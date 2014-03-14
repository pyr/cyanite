(ns leiningen.fatdeb
  "Build a .deb package from leininen, stolen from riemann"
  (:refer-clojure :exclude [replace])
  (:require [clojure.java.shell :refer [sh]]
            [clojure.java.io    :refer [file delete-file writer copy]]
            [clojure.string     :refer [join capitalize trim-newline replace]]
            [leiningen.uberjar  :refer [uberjar]])
  (:import java.text.SimpleDateFormat
           java.util.Date))

(defn md5
  [input]
  (let [digest (-> (doto (java.security.MessageDigest/getInstance "MD5")
                     (.reset)
                     (.update (.getBytes input)))
                   (.digest))]
    (.toString (java.math.BigInteger. 1 digest) 16)))

(defn delete-file-recursively
    "Delete file f. If it's a directory, recursively delete all its contents.
    Raise an exception if any deletion fails unless silently is true."
    [f & [silently]]
    (System/gc) ; This sometimes helps release files for deletion on windows.
    (let [f (file f)]
          (if (.isDirectory f)
                  (doseq [child (.listFiles f)]
                            (delete-file-recursively child silently)))
          (delete-file f silently)))

(defn deb-dir
  "Debian package working directory."
  [project]
  (file (:root project) "target/deb/cyanite"))

(defn cleanup
  [project]
  ; Delete working dir.
  (when (.exists (deb-dir project))
    (delete-file-recursively (deb-dir project))))

(defn reset
  [project]
  (cleanup project)
  (sh "rm" (str (:root project) "/target/*.deb")))

(def build-date (Date.))

(defn get-version
  [project]
  (let [df   (SimpleDateFormat. "yyyyMMdd-HHmmss")]
    (replace (:version project) #"SNAPSHOT" (.format df build-date))))

(defn control
  "Control file"
  [project]
  (join "\n"
        (map (fn [[k v]] (str (capitalize (name k)) ": " v))
             {:package (:name project)
              :version (get-version project)
              :section "base"
              :priority "optional"
              :architecture "all"
              :depends (join ", " ["bash"])
              :maintainer (:email (:maintainer project))
              :description (:description project)})))

(defn write
  "Write string to file, plus newline"
  [file string]
  (with-open [w (writer file)]
    (.write w (str (trim-newline string) "\n"))))

(defn make-deb-dir
  "Creates the debian package structure in a new directory."
  [project]
  (let [dir (deb-dir project)]
    (.mkdirs dir)

    ; Meta
    (.mkdirs (file dir "DEBIAN"))
    (write (file dir "DEBIAN" "control") (control project))
    (write (file dir "DEBIAN" "conffiles")
           (join "\n" ["/etc/cyanite.yaml"
                       "/etc/init.d/cyanite.yaml"
                       "/etc/default/cyanite"]))

    ; Preinst
    (copy (file (:root project) "pkg" "deb" "preinst.sh")
          (file dir "DEBIAN" "preinst"))
    (.setExecutable (file dir "DEBIAN" "preinst") true false)

    ; Postinst
    (copy (file (:root project) "pkg" "deb" "postinst.sh")
          (file dir "DEBIAN" "postinst"))
    (.setExecutable (file dir "DEBIAN" "postinst") true false)

    ; Prerm
    (copy (file (:root project) "pkg" "deb" "prerm.sh")
          (file dir "DEBIAN" "prerm"))
    (.setExecutable (file dir "DEBIAN" "prerm") true false)

    ; Jar
    (.mkdirs (file dir "usr" "lib" "cyanite"))
    (copy (file (:root project) "target"
                (str "cyanite-" (:version project) "-standalone.jar"))
          (file dir "usr" "lib" "cyanite" "cyanite.jar"))

    ; cql schema
    (.mkdirs (file dir "var" "lib" "cyanite"))
    (copy (file (:root project) "doc" "schema.cql")
          (file dir "var" "lib" "cyanite" "schema.cql"))

    ; Binary
    (.mkdirs (file dir "usr" "bin"))
    (copy (file (:root project) "pkg" "deb" "cyanite")
          (file dir "usr" "bin" "cyanite"))
    (.setExecutable (file dir "usr" "bin" "cyanite") true false)

    ; Log dir
    (.mkdirs (file dir "var" "log" "cyanite"))

    ; Config
    (.mkdirs (file dir "etc"))
    (copy (file (:root project) "doc" "cyanite.yaml")
          (file dir "etc" "cyanite.yaml"))

    ; defaults file
    (.mkdirs (file dir "etc" "default"))
    (copy (file (:root project) "pkg" "deb" "cyanite.default")
          (file dir "etc" "default" "cyanite"))

    ; Init script
    (.mkdirs (file dir "etc" "init.d"))
    (copy (file (:root project) "pkg" "deb" "init.sh")
          (file dir "etc" "init.d" "cyanite"))
    (.setExecutable (file dir "etc" "init.d" "cyanite") true false)

    dir))

(defn dpkg
  "Convert given package directory to a .deb."
  [project deb-dir]
  (print (:err (sh "dpkg" "--build"
                   (str deb-dir)
                   (str (file (:root project) "target")))))
  (let [deb-file-name (str (:name project) "_"
                           (get-version project) "_"
                           "all" ".deb")
        deb-file (file (:root project) "target" deb-file-name)]
    (write (str deb-file ".md5")
           (str (md5 (slurp deb-file)) " " deb-file-name))))

(defn fatdeb
  ([project]
   (reset project)
   (uberjar project)
   (dpkg project (make-deb-dir project))
   (cleanup project)
   (flush)))

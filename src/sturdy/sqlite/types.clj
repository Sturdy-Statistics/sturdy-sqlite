(ns sturdy.sqlite.types
  (:require
   [clojure.string :as string]

   [babashka.fs :as fs]
   [camel-snake-kebab.core :refer [->kebab-case]]

   [next.jdbc.result-set :as rs]
   [next.jdbc.prepare :as prepare]

   [taoensso.truss :refer [have]])
  (:import
   (java.util UUID)
   (java.nio.file Path)
   (clojure.lang Keyword)
   (java.nio ByteBuffer)
   (java.sql PreparedStatement ResultSet)))

(set! *warn-on-reflection* true)

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Type Conversions

(defn uuid->bytes ^bytes [^UUID uuid]
  (have uuid? uuid)
  (let [^ByteBuffer bb (ByteBuffer/wrap (byte-array 16))]
    (.putLong bb (.getMostSignificantBits uuid))
    (.putLong bb (.getLeastSignificantBits uuid))
    (.array bb)))

(defn bytes->uuid ^UUID [^bytes b]
  (let [^ByteBuffer bb (ByteBuffer/wrap b)]
    (UUID. (.getLong bb) (.getLong bb))))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Tell next.jdbc how to write custom types to SQLite (Global JVM level)

(extend-protocol prepare/SettableParameter
  ;; UUID -> BLOB
  UUID
  (set-parameter [^UUID v ^PreparedStatement s ^long i]
    (.setBytes s i (uuid->bytes v)))

  ;; Keyword -> TEXT
  Keyword
  (set-parameter [^Keyword v ^PreparedStatement s ^long i]
    (.setString s i (name v))))

;;; don't make private; tests need to reset it
(defonce path-base-dir* (atom nil))

(defn- convert-paths!
  [path-base-dir]

  ;; Path -> TEXT
  (let [normalized-base (-> path-base-dir fs/absolutize str)]
   (if (compare-and-set! path-base-dir* nil normalized-base)
     (extend-protocol prepare/SettableParameter
       Path
       (set-parameter [^Path v ^PreparedStatement s ^long i]
         (let [p (fs/relativize path-base-dir v)]
           (.setString s i (str p)))))

     (when (not= @path-base-dir* normalized-base)
       (throw (ex-info "Cannot set paths twice.  (Note that this feature cannot be used with parallel testing.)"
                       {:existing @path-base-dir*
                        :attempted path-base-dir}))))))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Tell next.jdbc how to read DB values back into Clojure types

(defn- make-column-parser
  "Returns a next.jdbc column parser function based on provided predicates.
   Predicates receive the kebab-cased column name."
  [{:keys [uuid-col? enum-col? bool-col? path-col?
           path-base-dir auto-convert-paths?
           json-col? json-parse-fn]}]

  (when (or (ifn? path-col?) auto-convert-paths?)
    (when-not (or (string? path-base-dir)
                  (instance? Path path-base-dir))
      (throw (ex-info "Unsupported type for path-base-dir.  Should be string or java.nio.file.Path."
                      {:type (type path-base-dir)}))))

  (when (and (ifn? json-col?)
             (not (fn? json-parse-fn)))
    (throw (ex-info "Must supply `json-parse-fn` if `json-col?` is supplied."
                    {})))

  (when auto-convert-paths?
    (convert-paths! path-base-dir))

  (fn [_builder ^ResultSet rs ^Integer i]
    (let [col-name (->kebab-case
                    (.getColumnLabel (.getMetaData rs) i))
          val      (.getObject rs i)]
      (cond
        ;; UUID
        (and (ifn? uuid-col?)
             (bytes? val)
             (= 16 (alength ^bytes val))
             (uuid-col? col-name))
        (bytes->uuid val)

        ;; Enum/Keyword
        (and (ifn? enum-col?)
             (string? val)
             (enum-col? col-name))
        (keyword val)

        ;; Boolean
        (and (ifn? bool-col?)
             (number? val)
             (bool-col? col-name))
        (= 1 val)

        ;; Paths
        (and (ifn? path-col?)
             (string? val)
             (not (string/blank? val))
             (path-col? col-name))
        (fs/path path-base-dir val)

        ;; JSON
        (and (ifn? json-col?)
             (ifn? json-parse-fn)
             (string? val)
             (json-col? col-name))
        (json-parse-fn val)

        ;; Default pass-through
        :else val))))

;;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; public API

(defn make-builder-opts
  "Returns next.jdbc builder-opts with custom column parsing."
  [parser-opts]
  {:builder-fn (rs/builder-adapter
                rs/as-unqualified-kebab-maps
                (make-column-parser parser-opts))})

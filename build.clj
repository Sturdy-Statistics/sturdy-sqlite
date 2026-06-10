(ns build
  (:require
   [clojure.string :as string]
   [clojure.tools.build.api :as b]
   [deps-deploy.deps-deploy :as dd]))

;;; Project coordinates and paths
(def lib 'com.sturdystats/sturdy-sqlite)
(def signing-key "7B071D29C214D038FD01F297BE9B4079D8721CDF")

(def basis     (b/create-basis {:project "deps.edn"}))
(def class-dir "target/classes")
(def target    "target")

;;; Git helpers
(defn git-describe-last-tag
  "Returns the last tag (string) or nil if none."
  []
  (try
    (let [out (b/git-process {:git-args ["describe" "--tags" "--abbrev=0"]})
          tag (some-> out string/trim)]
      (when (seq tag) tag))
    (catch Throwable _ nil)))

(defn git-short-sha []
  (-> (b/git-process {:git-args ["rev-parse" "--short" "HEAD"]})
      str
      string/trim))

(def commit-count (b/git-count-revs {}))

(defn normalize-tag->version [tag]
  ;; Strip common leading "v"
  (if (and tag (string/starts-with? tag "v"))
    (subs tag 1)
    tag))

(defn git-exact-tag []
  (try
    (let [out (b/git-process {:git-args ["describe" "--tags" "--exact-match"]})
          tag (some-> out string/trim)]
      (when (seq tag) tag))
    (catch Throwable _ nil)))

(def version
  (let [sha   (git-short-sha)
        exact (some-> (git-exact-tag) normalize-tag->version)
        last  (some-> (git-describe-last-tag) normalize-tag->version)]
    (cond
      exact exact
      last  (format "%s-g%s" last sha)
      :else (format "0.1.%s-g%s" commit-count sha))))

(def jar-file (format "%s/%s-%s.jar" target (name lib) version))

;;; Tasks

(defn clean
  "Delete the target/ directory."
  [_]
  (b/delete {:path target})
  (println "Cleaned" target))

(defn prepare
  [_]
  (b/copy-dir {:src-dirs ["src"]
               :target-dir class-dir}))

(defn jar
  "Create a thin JAR (not standalone) with a POM."
  [_]
  (clean nil)
  (prepare nil)

  (b/write-pom
   {:class-dir class-dir
    :lib       lib
    :version   version
    :basis     basis
    :src-dirs  ["src"]
    :pom-data
    [[:description "SQLite utility functions for Clojure apps."]
     [:url "https://github.com/Sturdy-Statistics/sturdy-sqlite"]
     [:licenses
      [:license
       [:name "Apache License 2.0"]
       [:url "https://www.apache.org/licenses/LICENSE-2.0"]]]
     [:scm
      [:tag (git-describe-last-tag)]
      [:url "https://github.com/Sturdy-Statistics/sturdy-sqlite"]
      [:connection "scm:git:https://github.com/Sturdy-Statistics/sturdy-sqlite.git"]]]})

  (b/jar {:class-dir class-dir
          :jar-file  jar-file})

  (println "Wrote jar:" jar-file)
  {:jar-file jar-file})

(defn deploy
  "Build and deploy to Clojars."
  [_]
  (when-not (git-exact-tag)
    (throw (ex-info "Refusing to deploy: not on an exact git tag" {})))
  (let [{:keys [jar-file]} (jar nil)]
    (dd/deploy
     {:installer      :remote
      :artifact       jar-file
      :pom-file       (b/pom-path {:class-dir class-dir :lib lib})
      :sign-releases? true
      :sign-key-id    signing-key})
    (println "Deployed jar:" jar-file)))

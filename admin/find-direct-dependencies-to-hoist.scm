#!/usr/bin/env guile
!#

;;; find-direct-dependencies-to-hoist.scm
;;;
;;; Reads `cargo metadata --format-version 1 --no-deps` JSON from stdin and
;;; reports dependencies declared directly (not via workspace inheritance) in
;;; `[dependencies]` by two or more workspace crates.  Such dependencies
;;; should be moved into the workspace Cargo.toml.
;;;
;;; The workspace root is passed as the first command-line argument so that
;;; absolute path dependencies can be displayed relative to the workspace.

(use-modules (ice-9 rdelim)
             (ice-9 regex)
             (json)
             (srfi srfi-1)
             (srfi srfi-13))

;;; Match an inline-table dependency declaration that sets workspace = true.
(define workspace-inline-re
  (make-regexp "^[[:space:]]*([A-Za-z0-9_-]+)[[:space:]]*=[[:space:]]*\\{([^}]*)workspace[[:space:]]*=[[:space:]]*true([^}]*)\\}"))

;;; Match package = "..." inside an inline-table body.
(define package-re
  (make-regexp "package[[:space:]]*=[[:space:]]*\"([^\"]+)\""))

(define (package-name-in-body body)
  (and body
       (let ((m (regexp-exec package-re body)))
         (and m (match:substring m 1)))))

;;; Match a key = value dependency line and capture the key and the raw RHS.
(define direct-key-re
  (make-regexp "^[[:space:]]*([A-Za-z0-9_-]+)[[:space:]]*=[[:space:]]*(.*)$"))

(define (strip-trailing-comment s)
  "Remove a trailing #comment from S, ignoring # inside double-quoted strings."
  (let ((len (string-length s)))
    (let loop ((i 0) (in-quote? #f))
      (cond
        ((>= i len) s)
        ((char=? (string-ref s i) #\")
         (loop (+ i 1) (not in-quote?)))
        ((and (not in-quote?)
              (char=? (string-ref s i) #\#))
         (string-trim-right (substring s 0 i)))
        (else
         (loop (+ i 1) in-quote?))))))

(define (direct-dep-name line)
  "If LINE declares a direct (non-workspace) dependency in [dependencies],
return the actual package name; otherwise return #f."
  (and (not (regexp-exec workspace-inline-re line))
       (let ((m (regexp-exec direct-key-re line)))
         (and m
              (let* ((key (match:substring m 1))
                     (rhs (strip-trailing-comment (match:substring m 2))))
                (or (package-name-in-body rhs) key))))))

(define (direct-dependency-names manifest-path)
  "Return the actual package names of all direct (non-workspace) [dependencies]
entries in MANIFEST-PATH."
  (call-with-input-file manifest-path
    (lambda (port)
      (let loop ((line (read-line port))
                 (in-deps? #f)
                 (names '()))
        (if (eof-object? line)
            (reverse names)
            (let* ((trimmed (string-trim-both line))
                   (len (string-length trimmed)))
              (cond
                ((or (zero? len)
                     (char=? (string-ref trimmed 0) #\#))
                 (loop (read-line port) in-deps? names))
                ((and (>= len 2)
                      (char=? (string-ref trimmed 0) #\[)
                      (char=? (string-ref trimmed (- len 1)) #\]))
                 (let ((section (string-trim-both
                                 (substring trimmed 1 (- len 1)))))
                   (loop (read-line port)
                         (string=? section "dependencies")
                         names)))
                (in-deps?
                 (let ((name (direct-dep-name line)))
                   (loop (read-line port)
                         in-deps?
                         (if name (cons name names) names))))
                (else
                 (loop (read-line port) in-deps? names)))))))))

(define (normal-dependency-map package)
  "Return a hash table mapping actual dependency name -> metadata dep record
for PACKAGE's normal dependencies."
  (let ((table (make-hash-table)))
    (for-each
     (lambda (dep)
       (when (and (eq? 'null (assoc-ref dep "kind"))
                  (eq? 'null (assoc-ref dep "target")))
         (hash-set! table (assoc-ref dep "name") dep)))
     (vector->list (assoc-ref package "dependencies")))
    table))

(define (dep-spec dep workspace-root)
  "Return a short human-readable spec string for DEP from cargo metadata."
  (let ((source (assoc-ref dep "source"))
        (req (assoc-ref dep "req"))
        (path (assoc-ref dep "path")))
    (cond
      ((and path (not source))
       (if (and (> (string-length workspace-root) 0)
                (string-prefix? workspace-root path))
           (string-append "path:" (substring path (+ (string-length workspace-root) 1)))
           (string-append "path:" path)))
      ((and source (string-prefix? "git+" source))
       (string-append "git:" (substring source 4)))
      (else
       req))))

(define (dedup-by-car lst)
  "Remove duplicate entries from LST, keeping the first entry for each car."
  (let loop ((seen '()) (result '()) (rest lst))
    (if (null? rest)
        (reverse result)
        (let ((key (caar rest)))
          (if (member key seen)
              (loop seen result (cdr rest))
              (loop (cons key seen) (cons (car rest) result) (cdr rest)))))))

(define (entry<? a b)
  (string<? (car a) (car b)))

(define (hoist<? a b)
  (string<? (car a) (car b)))

(define (main args)
  (let* ((workspace-root (if (>= (length args) 2)
                             (list-ref args 1)
                             ""))
         (data (json->scm (current-input-port)))
         (packages (vector->list (assoc-ref data "packages")))
         (member-ids (vector->list (assoc-ref data "workspace_members")))
         (members (filter (lambda (p) (member (assoc-ref p "id") member-ids))
                          packages))
         (member-names (map (lambda (p) (assoc-ref p "name")) members)))

    ;; dep-name -> list of (crate-name . spec-string)
    (define direct-table (make-hash-table))

    (for-each
     (lambda (crate)
       (let* ((crate-name (assoc-ref crate "name"))
              (manifest (assoc-ref crate "manifest_path"))
              (meta-map (normal-dependency-map crate)))
         (for-each
          (lambda (dep-name)
            (let ((dep (hash-ref meta-map dep-name #f)))
              (if dep
                  (hash-set! direct-table dep-name
                             (cons (cons crate-name (dep-spec dep workspace-root))
                                   (hash-ref direct-table dep-name '())))
                  (format (current-error-port)
                          "Warning: direct dependency ~a in ~a not found in cargo metadata~%"
                          dep-name crate-name))))
          (direct-dependency-names manifest))))
     members)

    ;; Filter for deps used directly by two or more crates, excluding workspace members.
    (define hoists
      (sort
       (hash-fold
        (lambda (dep-name entries acc)
          (let ((unique (dedup-by-car entries)))
            (if (and (>= (length unique) 2)
                     (not (member dep-name member-names)))
                (cons (cons dep-name (sort unique entry<?)) acc)
                acc)))
        '()
        direct-table)
       hoist<?))

    (if (null? hoists)
        (begin
          (display "No direct dependencies need to be hoisted to the workspace.\n")
          (exit 0))
        (begin
          (for-each
           (lambda (item)
             (let ((dep-name (car item))
                   (entries (cdr item)))
               (format #t "~a:~%" dep-name)
               (for-each
                (lambda (entry)
                  (format #t "  ~a: ~a~%" (car entry) (cdr entry)))
                entries)
               (newline)))
           hoists)
          (format #t "~a direct dependenc~a should be hoisted to the workspace Cargo.toml.~%"
                  (length hoists)
                  (if (= (length hoists) 1) "y" "ies"))
          (exit 1)))))

(main (command-line))

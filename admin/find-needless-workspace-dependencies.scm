#!/usr/bin/env guile
!#

;;; find-needless-workspace-dependencies.scm
;;;
;;; Reads `cargo metadata --format-version 1 --no-deps` JSON from stdin and
;;; reports workspace-inherited `[dependencies]` and `[dev-dependencies]` that
;;; are used by exactly one workspace crate.  Such dependencies can be moved
;;; out of the workspace Cargo.toml and into the single crate that needs them.

(use-modules (ice-9 rdelim)
             (ice-9 regex)
             (json)
             (srfi srfi-1)
             (srfi srfi-13))

;;; Match an inline-table dependency declaration that sets workspace = true.
;;; Captures:
;;;   1. the table key (the local dependency name),
;;;   2. the inline-table body before the workspace = true assignment,
;;;   3. the inline-table body after the workspace = true assignment.
(define workspace-inline-re
  (make-regexp "^[[:space:]]*([A-Za-z0-9_-]+)[[:space:]]*=[[:space:]]*\\{([^}]*)workspace[[:space:]]*=[[:space:]]*true([^}]*)\\}"))

;;; Match package = "..." inside an inline-table body.
(define package-re
  (make-regexp "package[[:space:]]*=[[:space:]]*\"([^\"]+)\""))

(define (package-name-in-body body)
  (and body
       (let ((m (regexp-exec package-re body)))
         (and m (match:substring m 1)))))

(define (workspace-inline-dep-name line)
  "If LINE declares a workspace = true inline-table dependency, return the
actual package name (honoring package = \"...\" renames); otherwise return #f."
  (let ((m (regexp-exec workspace-inline-re line)))
    (and m
         (let ((key (match:substring m 1))
               (before (match:substring m 2))
               (after (match:substring m 3)))
           (or (package-name-in-body before)
               (package-name-in-body after)
               key)))))

(define (workspace-dependency-names manifest-path section)
  "Return the actual package names of all SECTION entries (e.g.
\"dependencies\" or \"dev-dependencies\") in MANIFEST-PATH that are declared
with workspace = true."
  (call-with-input-file manifest-path
    (lambda (port)
      (let loop ((line (read-line port))
                 (in-section? #f)
                 (names '()))
        (if (eof-object? line)
            (reverse names)
            (let* ((trimmed (string-trim-both line))
                   (len (string-length trimmed)))
              (cond
                ;; Blank or comment line.
                ((or (zero? len)
                     (char=? (string-ref trimmed 0) #\#))
                 (loop (read-line port) in-section? names))

                ;; Section header.  We are interested only in the exact
                ;; requested section; any other header ends the scan.
                ((and (>= len 2)
                      (char=? (string-ref trimmed 0) #\[)
                      (char=? (string-ref trimmed (- len 1)) #\]))
                 (let ((current-section (string-trim-both
                                         (substring trimmed 1 (- len 1)))))
                   (loop (read-line port)
                         (string=? current-section section)
                         names)))

                ;; Inside the requested section, collect workspace = true entries.
                (in-section?
                 (let ((name (workspace-inline-dep-name line)))
                   (loop (read-line port)
                         in-section?
                         (if name (cons name names) names))))

                (else
                 (loop (read-line port) in-section? names)))))))))

(define (relevant-dependency-names package)
  "Return the names of normal and dev (but not build or target-specific)
dependencies of PACKAGE as reported by cargo metadata."
  (filter-map
   (lambda (dep)
     (let ((kind (assoc-ref dep "kind"))
           (target (assoc-ref dep "target")))
       (and (eq? 'null target)
            (or (eq? 'null kind)
                (string=? "dev" kind))
            (assoc-ref dep "name"))))
   (vector->list (assoc-ref package "dependencies"))))

(define (needless<? a b)
  (if (string=? (car a) (car b))
      (string<? (cdr a) (cdr b))
      (string<? (car a) (car b))))

(define (main args)
  (let* ((data (json->scm (current-input-port)))
         (packages (vector->list (assoc-ref data "packages")))
         (member-ids (vector->list (assoc-ref data "workspace_members")))
         (members (filter (lambda (p) (member (assoc-ref p "id") member-ids))
                          packages))
         (member-names (map (lambda (p) (assoc-ref p "name")) members)))

    ;; dep-name -> list of crate names that use it as a normal or dev dependency
    (define usage-table (make-hash-table))
    ;; dep-name -> list of crate names that declare it workspace = true
    (define workspace-table (make-hash-table))

    (for-each
     (lambda (crate)
       (let ((crate-name (assoc-ref crate "name"))
             (manifest (assoc-ref crate "manifest_path")))
         (for-each
          (lambda (dep-name)
            (hash-set! usage-table dep-name
                       (cons crate-name
                             (hash-ref usage-table dep-name '()))))
          (relevant-dependency-names crate))
         (for-each
          (lambda (dep-name)
            (hash-set! workspace-table dep-name
                       (cons crate-name
                             (hash-ref workspace-table dep-name '()))))
          (append
           (workspace-dependency-names manifest "dependencies")
           (workspace-dependency-names manifest "dev-dependencies")))))
     members)

    ;; A workspace dependency is "needless" when:
    ;;   - it is declared with workspace = true in exactly one crate, and
    ;;   - that same crate is the only workspace member using it at all, and
    ;;   - it is not itself another workspace member.
    (define needless
      (sort
       (hash-fold
        (lambda (dep-name workspace-users acc)
          (if (= (length workspace-users) 1)
              (let ((all-users (hash-ref usage-table dep-name '())))
                (if (and (= (length all-users) 1)
                         (string=? (car all-users) (car workspace-users))
                         (not (member dep-name member-names)))
                    (cons (cons (car workspace-users) dep-name) acc)
                    acc))
              acc))
        '()
        workspace-table)
       needless<?))

    (if (null? needless)
        (begin
          (display "No needless workspace dependencies found.\n")
          (exit 0))
        (begin
          (let loop ((items needless)
                     (current-crate #f))
            (unless (null? items)
              (let ((crate (caar items))
                    (dep (cdar items)))
                (when (not (equal? crate current-crate))
                  (format #t "~a:~%" crate))
                (format #t "  ~a~%" dep)
                (loop (cdr items) crate))))
          (format #t "~%~a needless workspace dependenc~a found.~%"
                  (length needless)
                  (if (= (length needless) 1) "y" "ies"))
          (exit 1)))))

(main (command-line))

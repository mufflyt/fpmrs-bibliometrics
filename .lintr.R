# Lint configuration -- a correctness gate, not a style gate.
#
# The pipeline is a single ~18k-line script written over time. Enforcing the
# full default linter set would produce thousands of cosmetic findings, take
# many minutes, and train everyone to ignore the job. Each linter enabled
# below catches a class of defect that has actually shipped in this project
# or would silently corrupt results.
#
# Adding a style linter is welcome, but only in a commit that also makes the
# codebase pass it.

linters <- linters_with_defaults(
  defaults = list(),

  # Format/argument mismatches. Every abstract sentence is built with
  # sprintf(), and a "%d" fed a fractional ratio printed "1x" for a true
  # value of 1.4 and "0x" for anything under 0.5.
  sprintf_linter = sprintf_linter(),

  # 1:length(x) counts down from 1 to 0 when x is empty.
  seq_linter = seq_linter(),

  # x == NA is always NA, never TRUE.
  equals_na_linter = equals_na_linter(),

  # T and F can be rebound; TRUE and FALSE cannot.
  T_and_F_symbol_linter = T_and_F_symbol_linter(),

  # & inside if() silently evaluates only the first element.
  vector_logic_linter = vector_logic_linter(),

  # f(x, , y) is almost always a stray comma.
  missing_argument_linter = missing_argument_linter(),

  # The same argument supplied twice; the later one silently wins. This
  # caught a duplicated `max_citations =` inside one summarise() call.
  duplicate_argument_linter = duplicate_argument_linter(),

  any_is_na_linter = any_is_na_linter(),

  # Hard-coded machine paths break every other machine, including CI.
  absolute_path_linter = absolute_path_linter(),

  # Code after return()/stop() never runs.
  unreachable_code_linter = unreachable_code_linter()
)

# Test fixtures deliberately contain odd values; CI helper scripts are not
# part of the analysis surface.
exclusions <- list("tests", "inst/ci")

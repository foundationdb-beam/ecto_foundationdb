%{
  configs: [
    %{
      name: "default",
      checks: %{
        disabled: [
          {Credo.Check.Readability.ParenthesesOnZeroArityDefs, []},
          {Credo.Check.Refactor.CyclomaticComplexity, []},
          {Credo.Check.Refactor.Apply, []},
          {Credo.Check.Design.AliasUsage, excluded_lastnames: [FoundationDB]},
          {Credo.Check.Refactor.FunctionArity, []},
          {Credo.Check.Refactor.Nesting, max_nesting: 3}
        ]
      }
    }
  ]
}

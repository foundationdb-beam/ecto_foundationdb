%{
  configs: [
    %{
      name: "default",
      checks: %{
        disabled: [
          {Credo.Check.Readability.ParenthesesOnZeroArityDefs, []},
          {Credo.Check.Refactor.CyclomaticComplexity, []},
          {Credo.Check.Refactor.Apply, []}
        ]
      }
    }
  ]
}

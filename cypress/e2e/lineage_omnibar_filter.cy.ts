/// <reference types="cypress" />

describe("Omnibar filter", () => {
  it("applies global filters to matching tables unless locally overridden", () => {
    cy.viewport(1000, 800);

    const configPath = `${Cypress.config("projectRoot")}/example/duckdb_example/kptn.yaml`;
    const requests: Record<string, string> = {};

    cy.intercept("POST", "/table-preview-query", (req) => {
      const table = req.body?.table || "unknown";
      requests[table] = req.body?.sql || "";
      req.reply({ statusCode: 200, body: { columns: ["id"], row: [1] } });
    }).as("previewRequests");

    cy.visit(`/lineage-page?configPath=${encodeURIComponent(configPath)}`);

    // Enable global filter and set clause.
    cy.get("#kptn-filter-all").click();
    cy.get("#kptn-global-filter")
      .should("be.visible")
      .clear()
      .type("id = 2; id = 3");

    // Override one table locally.
    cy.contains(".table-name", "main.fruit_metrics")
      .parents(".table")
      .within(() => {
        cy.get(".kptn-toggle-sql").click();
        cy.get(".kptn-sql-input input").clear().type("id = 99");
      });

    // Trigger previews for all tables.
    cy.get("#kptn-preview-all").click();
    cy.wait("@previewRequests");
    cy.wait(500);

    // Tables that have an id column should include the global filter.
    const expectGlobal = [
      "main.raw_numbers",
      "main.fruit_metrics",
      "main.python_source_table",
      "main.python_consumer",
    ];

    cy.wrap(null).should(() => {
      expect(Object.keys(requests).length).to.be.greaterThan(3);
      expectGlobal.forEach((table) => {
        expect(requests[table]).to.exist;
      });

      // Global filter applied where no local override.
      const orPattern = /where\s+\(?.*id\s*=\s*2.*or.*id\s*=\s*3.*\)?/i;
      expect(requests["main.raw_numbers"]).to.match(orPattern);
      expect(requests["main.python_source_table"]).to.match(orPattern);
      expect(requests["main.python_consumer"]).to.match(orPattern);

      // Local filter overrides global.
      expect(requests["main.fruit_metrics"]).to.match(/where\s+id\s*=\s*99/i);

      // Table without matching column should not receive the global clause.
      expect(requests["main.wide_table"] || "").to.not.match(/where\s+id\s*=/i);
    });
  });
});

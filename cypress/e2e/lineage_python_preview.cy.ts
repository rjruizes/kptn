/// <reference types="cypress" />

describe("Python task preview columns", () => {
  it("requests inferred columns instead of SELECT * for python task outputs", () => {
    cy.viewport(900, 700);

    const configPath = `${Cypress.config("projectRoot")}/example/duckdb_example/kptn.yaml`;
    const tableName = "main.python_source_table";

    cy.intercept("POST", "/table-preview-query", (req) => {
      if (req.body?.table === tableName) {
        expect(req.body.sql.toUpperCase()).to.not.contain("SELECT *");
        expect(req.body.sql).to.match(/select\s+.*id.*payload/i);
      }
      req.reply({ statusCode: 200, body: { columns: ["id", "payload"], row: [1, "payload"] } });
    }).as("pythonPreview");

    cy.visit(`/lineage-page?configPath=${encodeURIComponent(configPath)}`);

    cy.contains(".table-name", tableName, { timeout: 15000 })
      .parents(".table")
      .within(() => {
        cy.get(".kptn-open-table").click();
      });

    cy.wait("@pythonPreview");
  });
});

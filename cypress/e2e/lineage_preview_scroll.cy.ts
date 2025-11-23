/// <reference types="cypress" />

describe("Lineage preview layout", () => {
  it("keeps wide preview tables horizontally scrollable", () => {
    cy.viewport(900, 700);

    const configPath = `${Cypress.config("projectRoot")}/example/duckdb_example/kptn.yaml`;
    cy.visit(`/lineage-page?configPath=${encodeURIComponent(configPath)}`);

    cy.contains(".table-name", "main.wide_table", { timeout: 15000 })
      .parents(".table")
      .find("th.column")
      .should("have.length", 20)
      .each(($th) => {
        expect($th.text()).to.have.length(16);
      });

    cy.get("#visualizer")
      .should("have.css", "overflow-x", "auto")
      .then(($el) => {
        const el = $el[0];
        expect(el.scrollWidth).to.be.greaterThan(el.clientWidth);
      });
  });
});

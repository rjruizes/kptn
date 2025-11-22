/// <reference types="cypress" />

describe("Lineage hover interactions", () => {
  it("shows dependency edges on column hover", () => {
    Cypress.on("uncaught:exception", (err) => {
      if (err.message.includes("Invalid regular expression flags")) {
        return false;
      }
      return undefined;
    });

    const configPath = `${Cypress.config("projectRoot")}/example/duckdb_example/kptn.yaml`;
    cy.visit(
      `/lineage-page?configPath=${encodeURIComponent(configPath)}`,
    );

    cy.get(".lineage-path", { timeout: 15000 }).should(
      "have.length.greaterThan",
      0,
    );

    cy.document().then((doc) => {
      const columns = Array.from(doc.querySelectorAll(".column"));
      expect(columns.length).to.be.greaterThan(0);

      let hasVisibleEdge = false;
      columns.forEach((column) => {
        column.dispatchEvent(new Event("mouseenter", { bubbles: true }));
        if (doc.querySelectorAll(".lineage-path.visible").length > 0) {
          hasVisibleEdge = true;
        }
        column.dispatchEvent(new Event("mouseleave", { bubbles: true }));
      });

      expect(hasVisibleEdge).to.be.true;
    });
  });
});

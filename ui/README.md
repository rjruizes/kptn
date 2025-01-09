# UI

This UI exists to debug and manage data pipelines, both in the cloud and locally. 

The deployment environment is called the stack. You can point at a local stack (containers) or a cloud stack.

The UI also allows you to copy state from one stack to another. You can copy cloud state to local to reproduce and debug issues.

## UI implementation

The frontend SPA depends on a backend Python server for filesystem operations, API requests for the cloud, and local Docker API requests including building, starting, and stopping containers. The local backend server depends on a cloud server for copying files in EFS.

## UI deps

React was chosen because it has the greatest compatibility with other packages

- SPA Framework: [React 18](https://react.dev/)
- Bundler: [vite](https://vite.dev/)
- CSS: [Tailwind](tailwindcss.com)
- Components: [shadcn/ui](https://ui.shadcn.com/)
- Fetch: [mande](https://github.com/posva/mande)
- Router: [TanStack Router](https://tanstack.com/router)
- Icons: [FontAwesome](https://github.com/FortAwesome/react-fontawesome)


# React + TypeScript + Vite

This template provides a minimal setup to get React working in Vite with HMR and some ESLint rules.

Currently, two official plugins are available:

- [@vitejs/plugin-react](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react/README.md) uses [Babel](https://babeljs.io/) for Fast Refresh
- [@vitejs/plugin-react-swc](https://github.com/vitejs/vite-plugin-react-swc) uses [SWC](https://swc.rs/) for Fast Refresh

## Expanding the ESLint configuration

If you are developing a production application, we recommend updating the configuration to enable type aware lint rules:

- Configure the top-level `parserOptions` property like this:

```js
export default tseslint.config({
  languageOptions: {
    // other options...
    parserOptions: {
      project: ['./tsconfig.node.json', './tsconfig.app.json'],
      tsconfigRootDir: import.meta.dirname,
    },
  },
})
```

- Replace `tseslint.configs.recommended` to `tseslint.configs.recommendedTypeChecked` or `tseslint.configs.strictTypeChecked`
- Optionally add `...tseslint.configs.stylisticTypeChecked`
- Install [eslint-plugin-react](https://github.com/jsx-eslint/eslint-plugin-react) and update the config:

```js
// eslint.config.js
import react from 'eslint-plugin-react'

export default tseslint.config({
  // Set the react version
  settings: { react: { version: '18.3' } },
  plugins: {
    // Add the react plugin
    react,
  },
  rules: {
    // other rules...
    // Enable its recommended rules
    ...react.configs.recommended.rules,
    ...react.configs['jsx-runtime'].rules,
  },
})
```

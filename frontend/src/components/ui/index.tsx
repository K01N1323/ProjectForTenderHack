import React from 'react';

export const Button = ({ label }: { label: string }) => (
  <button>{label}</button>
);

export const Input = (props: React.InputHTMLAttributes<HTMLInputElement>) => (
  <input {...props} />
);

export const Loader = () => <div>Loading...</div>;

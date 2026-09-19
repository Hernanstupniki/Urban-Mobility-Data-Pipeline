# Instrucciones para agentes

## Registro de errores

Antes de diagnosticar un fallo, consultar `docs/troubleshooting.md` y comprobar si el síntoma ya está registrado.

Cuando aparezca un error nuevo:

1. agregar una entrada al registro con estado `DETECTADO` antes de cerrar el trabajo, aunque todavía no exista solución;
2. incluir fecha, componente, síntoma literal, evidencia comprobada, causa sólo si fue confirmada y archivos/configuración implicados;
3. no presentar conjeturas como causas;
4. cuando se valide una corrección, actualizar la misma entrada a `RESUELTO`, documentar el cambio y la prueba que pasó;
5. si la validación quedó interrumpida o incompleta, usar `PENDIENTE` y describir exactamente qué falta;
6. nunca registrar contraseñas, tokens ni otros secretos.

Mantener el registro enfocado en incidentes reproducibles y soluciones operativas. No eliminar antecedentes resueltos: sirven para evitar regresiones.

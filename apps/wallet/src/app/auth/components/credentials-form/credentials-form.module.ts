import { NgModule } from '@angular/core';
import { CrendetialsFormComponent } from './credentials-form.component';
import { ReactiveFormsModule } from '@angular/forms';
import { MatFormFieldModule, MatInputModule } from '@angular/material';
import { FlexLayoutModule } from '@angular/flex-layout';

@NgModule({
  imports: [ReactiveFormsModule, FlexLayoutModule, MatFormFieldModule, MatInputModule],
  declarations: [CrendetialsFormComponent],
  exports: [CrendetialsFormComponent]
})
export class CredentialsFormModule {}
